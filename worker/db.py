import asyncio
import logging
import os

import asyncpg

logger = logging.getLogger('worker.db')

POSTGRES_USER = os.environ.get('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'loadbalancer')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.environ.get('POSTGRES_PORT', '5432'))

_pool = None


async def get_pool():
    global _pool
    if _pool is None:
        for attempt in range(30):
            try:
                _pool = await asyncpg.create_pool(
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    database=POSTGRES_DB,
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    min_size=2,
                    max_size=10
                )
                logger.info('Successfully connected to PostgreSQL')
                return _pool
            except Exception as e:
                logger.warning(f'DB connection attempt {attempt+1}/30 failed: {e}')
                await asyncio.sleep(1)
        raise RuntimeError('Failed to connect to database after 30 attempts')
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS results (
                id SERIAL PRIMARY KEY,
                job_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'completed',
                result_data TEXT,
                worker_id TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        ''')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_results_job_id ON results(job_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_results_batch_id ON results(batch_id)')
    logger.info('Database tables initialized')


async def insert_result_no_lock(job_id, batch_id, result_data, worker_id, timeout=None):
    """Insert result without any locking — vulnerable to write skew."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # READ: check count
            count = await conn.fetchval(
                'SELECT COUNT(*) FROM results WHERE batch_id = $1',
                batch_id,
                timeout=timeout
            )
            logger.info(f'[NO_LOCK] batch_id={batch_id} count={count}')
            
            threshold = int(os.environ.get('BATCH_THRESHOLD', '10'))
            if count < threshold:
                # WRITE: insert if under threshold
                await conn.execute(
                    '''INSERT INTO results (job_id, batch_id, status, result_data, worker_id)
                       VALUES ($1, $2, $3, $4, $5)''',
                    job_id, batch_id, 'completed', result_data, worker_id,
                    timeout=timeout
                )
                logger.info(f'[NO_LOCK] Inserted result for job_id={job_id}, batch_id={batch_id}')
                return True
            else:
                logger.info(f'[NO_LOCK] Threshold reached for batch_id={batch_id}, skipping insert')
                return False


async def insert_result_pessimistic(job_id, batch_id, result_data, worker_id, timeout=None):
    """Insert result with pessimistic locking (SELECT FOR UPDATE).
    
    Uses pg_advisory_xact_lock to serialize access to the batch, then
    selects rows FOR UPDATE to hold locks until commit.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            import hashlib
            h = hashlib.sha256(batch_id.encode('utf-8')).digest()
            batch_hash = int.from_bytes(h[:8], byteorder='big') & 0x7FFFFFFFFFFFFFFF
            await conn.execute(
                'SELECT pg_advisory_xact_lock($1)',
                batch_hash,
                timeout=timeout
            )
            
            # Now we have exclusive access for this batch_id
            # Lock existing rows with FOR UPDATE and count them
            rows = await conn.fetch(
                'SELECT id FROM results WHERE batch_id = $1 FOR UPDATE',
                batch_id,
                timeout=timeout
            )
            count = len(rows)
            logger.info(f'[PESSIMISTIC] batch_id={batch_id} count={count}')
            
            threshold = int(os.environ.get('BATCH_THRESHOLD', '10'))
            if count < threshold:
                await conn.execute(
                    '''INSERT INTO results (job_id, batch_id, status, result_data, worker_id)
                       VALUES ($1, $2, $3, $4, $5)''',
                    job_id, batch_id, 'completed', result_data, worker_id,
                    timeout=timeout
                )
                logger.info(f'[PESSIMISTIC] Inserted result for job_id={job_id}')
                return True
            else:
                logger.info(f'[PESSIMISTIC] Threshold reached for batch_id={batch_id}')
                return False


async def insert_result_optimistic(job_id, batch_id, result_data, worker_id, timeout=None, max_retries=10):
    """Insert result with optimistic locking (version-based with SERIALIZABLE).
    
    Uses SERIALIZABLE isolation level. If a serialization failure occurs
    (indicating a concurrent modification), the transaction is retried.
    """
    pool = await get_pool()
    threshold = int(os.environ.get('BATCH_THRESHOLD', '10'))
    
    for attempt in range(max_retries):
        async with pool.acquire() as conn:
            try:
                async with conn.transaction(isolation='serializable'):
                    # Read current count
                    count = await conn.fetchval(
                        'SELECT COUNT(*) FROM results WHERE batch_id = $1',
                        batch_id,
                        timeout=timeout
                    )
                    
                    logger.info(f'[OPTIMISTIC] attempt={attempt+1} batch_id={batch_id} count={count}')
                    
                    if count >= threshold:
                        logger.info(f'[OPTIMISTIC] Threshold reached for batch_id={batch_id}')
                        return False
                    
                    # Insert — if another transaction modified the same data,
                    # PostgreSQL will raise a serialization error on commit
                    await conn.execute(
                        '''INSERT INTO results (job_id, batch_id, status, result_data, worker_id, version)
                           VALUES ($1, $2, $3, $4, $5, $6)''',
                        job_id, batch_id, 'completed', result_data, worker_id, count + 1,
                        timeout=timeout
                    )
                    logger.info(f'[OPTIMISTIC] Inserted result for job_id={job_id} version={count + 1}')
                    return True
            except asyncpg.SerializationError:
                logger.info(f'[OPTIMISTIC] Serialization conflict, retrying (attempt {attempt+1})')
                await asyncio.sleep(0.01 * (attempt + 1))
                continue
            except asyncpg.DeadlockDetectedError:
                logger.info(f'[OPTIMISTIC] Deadlock detected, retrying (attempt {attempt+1})')
                await asyncio.sleep(0.02 * (attempt + 1))
                continue
    
    logger.warning(f'[OPTIMISTIC] Max retries reached for job_id={job_id}')
    return False


async def insert_result(job_id, batch_id, result_data, worker_id, locking_mode='none', timeout=None):
    """Dispatch to the appropriate locking strategy."""
    if locking_mode == 'pessimistic':
        return await insert_result_pessimistic(job_id, batch_id, result_data, worker_id, timeout=timeout)
    elif locking_mode == 'optimistic':
        return await insert_result_optimistic(job_id, batch_id, result_data, worker_id, timeout=timeout)
    else:
        return await insert_result_no_lock(job_id, batch_id, result_data, worker_id, timeout=timeout)


async def simple_insert(job_id, batch_id, result_data, worker_id, timeout=None):
    """Simple insert without any batch threshold logic. Used for deadline propagation tests."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO results (job_id, batch_id, status, result_data, worker_id)
               VALUES ($1, $2, $3, $4, $5)''',
            job_id, batch_id, 'completed', result_data, worker_id,
            timeout=timeout
        )
        logger.info(f'Inserted result for job_id={job_id}')


async def get_batch_count(batch_id):
    """Get the count of results for a batch."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(
            'SELECT COUNT(*) FROM results WHERE batch_id = $1',
            batch_id
        )


async def get_result_by_job_id(job_id):
    """Get a result by job_id."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            'SELECT * FROM results WHERE job_id = $1',
            job_id
        )


async def clear_batch(batch_id):
    """Delete all results for a batch (for testing)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('DELETE FROM results WHERE batch_id = $1', batch_id)


async def clear_all():
    """Delete all results (for testing)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('DELETE FROM results')
