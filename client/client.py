import asyncio
import argparse
import logging
import os
import sys
import time
import uuid
from collections import defaultdict

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

sys.path.insert(0, '/app/generated')
sys.path.insert(0, '/app')

import worker_pb2
import worker_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('client')


class RoundRobinLoadBalancer:
    """Client-side load balancer with health checking."""
    
    def __init__(self, addresses):
        self.all_addresses = list(addresses)
        self.healthy_addresses = list(addresses)
        self.channels = {}
        self.stubs = {}
        self.health_stubs = {}
        self.current_index = 0
        self._lock = asyncio.Lock()
        self._health_check_task = None
        self._running = True
        
        # Create channels and stubs for all addresses
        for addr in self.all_addresses:
            channel = grpc.aio.insecure_channel(addr)
            self.channels[addr] = channel
            self.stubs[addr] = worker_pb2_grpc.WorkerServiceStub(channel)
            self.health_stubs[addr] = health_pb2_grpc.HealthStub(channel)
        
        logger.info(f'Load balancer initialized with {len(self.all_addresses)} workers: '
                   f'{", ".join(self.all_addresses)}')
    
    async def start_health_checks(self, interval=2.0):
        """Start periodic health checking."""
        self._health_check_task = asyncio.create_task(
            self._health_check_loop(interval)
        )
        logger.info(f'Health check loop started (interval={interval}s)')
    
    async def _health_check_loop(self, interval):
        while self._running:
            await self._check_all_health()
            await asyncio.sleep(interval)
    
    async def _check_all_health(self):
        tasks = []
        for addr in self.all_addresses:
            tasks.append(self._check_health(addr))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        async with self._lock:
            new_healthy = []
            for addr, result in zip(self.all_addresses, results):
                if result is True:
                    new_healthy.append(addr)
                    if addr not in self.healthy_addresses:
                        logger.info(f'Worker {addr} is now HEALTHY — adding to rotation')
                else:
                    if addr in self.healthy_addresses:
                        logger.warning(f'Worker {addr} is now UNHEALTHY — removing from rotation')
            
            if set(new_healthy) != set(self.healthy_addresses):
                self.healthy_addresses = new_healthy
                if self.current_index >= len(self.healthy_addresses):
                    self.current_index = 0
                logger.info(f'Active workers: {self.healthy_addresses}')
    
    async def _check_health(self, addr):
        try:
            stub = self.health_stubs[addr]
            response = await stub.Check(
                health_pb2.HealthCheckRequest(),
                timeout=2.0
            )
            return response.status == health_pb2.HealthCheckResponse.SERVING
        except Exception as e:
            logger.debug(f'Health check failed for {addr}: {e}')
            return False
    
    async def get_stub(self):
        """Get the next healthy worker stub using round-robin."""
        async with self._lock:
            if not self.healthy_addresses:
                raise grpc.aio.AioRpcError(
                    grpc.StatusCode.UNAVAILABLE,
                    initial_metadata=grpc.aio.Metadata(),
                    trailing_metadata=grpc.aio.Metadata(),
                    details='No healthy workers available'
                )
            
            addr = self.healthy_addresses[self.current_index % len(self.healthy_addresses)]
            self.current_index = (self.current_index + 1) % len(self.healthy_addresses)
            return self.stubs[addr], addr
    
    async def close(self):
        self._running = False
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        for channel in self.channels.values():
            await channel.close()


async def send_job(lb, job_id, batch_id='default', payload='test_payload', timeout=5.0):
    """Send a single ProcessJob request."""
    try:
        stub, addr = await lb.get_stub()
        logger.info(f'Sending job {job_id} to {addr} (timeout={timeout}s)')
        
        request = worker_pb2.JobRequest(
            job_id=job_id,
            batch_id=batch_id,
            payload=payload
        )
        
        response = await stub.ProcessJob(request, timeout=timeout)
        logger.info(f'Job {job_id} completed on {response.worker_id}: '
                   f'status={response.status}')
        return response
    except grpc.aio.AioRpcError as e:
        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            logger.error(f'Job {job_id} DEADLINE_EXCEEDED on {addr}')
        elif e.code() == grpc.StatusCode.UNAVAILABLE:
            logger.error(f'Job {job_id} UNAVAILABLE: {e.details()}')
        else:
            logger.error(f'Job {job_id} error: {e.code()} - {e.details()}')
        raise


async def test_wasted_work(lb):
    """Phase 2: Demonstrate wasted work with 2s deadline and 3s enrichment."""
    logger.info('=== TEST: Wasted Work (Phase 2) ===')
    job_id = f'wasted-{uuid.uuid4().hex[:8]}'
    
    try:
        await send_job(lb, job_id, batch_id='wasted-test', timeout=2.0)
        logger.info(f'Job {job_id} unexpectedly succeeded')
    except grpc.aio.AioRpcError as e:
        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            logger.info(f'Client correctly received DEADLINE_EXCEEDED for {job_id}')
        else:
            logger.error(f'Unexpected error: {e}')
    
    # Wait for server to finish processing (3.5s total)
    logger.info('Waiting 3.5s for server to finish processing...')
    await asyncio.sleep(3.5)
    
    # Check database
    import asyncpg
    conn = await asyncpg.connect(
        user=os.environ.get('POSTGRES_USER', 'postgres'),
        password=os.environ.get('POSTGRES_PASSWORD', 'postgres'),
        database=os.environ.get('POSTGRES_DB', 'loadbalancer'),
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        port=int(os.environ.get('POSTGRES_PORT', '5432'))
    )
    row = await conn.fetchrow('SELECT * FROM results WHERE job_id = $1', job_id)
    await conn.close()
    
    if row:
        logger.info(f'WASTED WORK CONFIRMED: Found row in DB for timed-out job {job_id}')
        logger.info(f'  DB Row: job_id={row["job_id"]}, worker_id={row["worker_id"]}, '
                   f'created_at={row["created_at"]}')
        return True
    else:
        logger.info(f'No wasted work: No row found for job {job_id}')
        return False


async def test_deadline_fix(lb):
    """Phase 3: Verify deadline propagation prevents wasted work."""
    logger.info('=== TEST: Deadline Propagation Fix (Phase 3) ===')
    job_id = f'deadline-fix-{uuid.uuid4().hex[:8]}'
    
    try:
        await send_job(lb, job_id, batch_id='deadline-test', timeout=2.0)
        logger.info(f'Job {job_id} unexpectedly succeeded')
    except grpc.aio.AioRpcError as e:
        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            logger.info(f'Client correctly received DEADLINE_EXCEEDED for {job_id}')
    
    logger.info('Waiting 3.5s to verify no DB write...')
    await asyncio.sleep(3.5)
    
    import asyncpg
    conn = await asyncpg.connect(
        user=os.environ.get('POSTGRES_USER', 'postgres'),
        password=os.environ.get('POSTGRES_PASSWORD', 'postgres'),
        database=os.environ.get('POSTGRES_DB', 'loadbalancer'),
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        port=int(os.environ.get('POSTGRES_PORT', '5432'))
    )
    row = await conn.fetchrow('SELECT * FROM results WHERE job_id = $1', job_id)
    await conn.close()
    
    if row:
        logger.error(f'DEADLINE FIX FAILED: Found row in DB for timed-out job {job_id}')
        return False
    else:
        logger.info(f'DEADLINE FIX VERIFIED: No row found for timed-out job {job_id}')
        return True


async def test_load_balancing(lb):
    """Phase 5: Test round-robin distribution."""
    logger.info('=== TEST: Load Balancing (Phase 5) ===')
    worker_counts = defaultdict(int)
    
    for i in range(12):
        job_id = f'lb-test-{i}'
        try:
            response = await send_job(lb, job_id, batch_id=f'lb-batch', timeout=10.0)
            worker_counts[response.worker_id] += 1
        except Exception as e:
            logger.error(f'Job {job_id} failed: {e}')
    
    logger.info(f'Load distribution: {dict(worker_counts)}')
    return len(worker_counts) > 1


async def test_total_outage(lb):
    """Phase 5: Test behavior when all workers are down."""
    logger.info('=== TEST: Total Outage ===')
    # Force unhealthy state
    async with lb._lock:
        lb.healthy_addresses = []
    
    start = time.time()
    try:
        await send_job(lb, 'outage-test', timeout=5.0)
        logger.error('Should have failed with UNAVAILABLE')
        return False
    except grpc.aio.AioRpcError as e:
        elapsed = time.time() - start
        if e.code() == grpc.StatusCode.UNAVAILABLE and elapsed < 1.0:
            logger.info(f'TOTAL OUTAGE TEST PASSED: Got UNAVAILABLE in {elapsed:.3f}s')
            return True
        else:
            logger.error(f'Unexpected: code={e.code()}, elapsed={elapsed:.3f}s')
            return False
    except Exception as e:
        elapsed = time.time() - start
        if elapsed < 1.0:
            logger.info(f'TOTAL OUTAGE TEST PASSED: Failed fast in {elapsed:.3f}s: {e}')
            return True
        return False


async def continuous_mode(lb):
    """Run continuous jobs for failure injection testing."""
    logger.info('=== MODE: Continuous (for failure injection testing) ===')
    i = 0
    while True:
        job_id = f'continuous-{i}'
        try:
            stub, addr = await lb.get_stub()
            logger.info(f'Sending job {job_id} to {addr}')
            request = worker_pb2.JobRequest(
                job_id=job_id,
                batch_id='continuous',
                payload=f'data_{i}'
            )
            response = await stub.ProcessJob(request, timeout=10.0)
            logger.info(f'Job {job_id} completed on {response.worker_id}')
        except grpc.aio.AioRpcError as e:
            logger.error(f'Job {job_id} failed: {e.code()}')
        except Exception as e:
            logger.error(f'Job {job_id} error: {e}')
        
        i += 1
        await asyncio.sleep(1)


async def main():
    parser = argparse.ArgumentParser(description='gRPC Client with Load Balancing')
    parser.add_argument('--mode', default='wait',
                       choices=['wait', 'wasted-work', 'deadline-fix', 
                               'load-balance', 'continuous', 'total-outage', 'all'],
                       help='Test mode to run')
    parser.add_argument('--workers', default=os.environ.get(
        'WORKER_ADDRESSES', 'worker-1:50051,worker-2:50051,worker-3:50051'),
                       help='Comma-separated worker addresses')
    
    args = parser.parse_args()
    
    addresses = [addr.strip() for addr in args.workers.split(',')]
    lb = RoundRobinLoadBalancer(addresses)
    await lb.start_health_checks(interval=2.0)
    
    # Wait for initial health checks
    await asyncio.sleep(3.0)
    
    try:
        if args.mode == 'wait':
            logger.info('Client is in wait mode. Use docker exec to run tests.')
            while True:
                await asyncio.sleep(60)
        elif args.mode == 'wasted-work':
            result = await test_wasted_work(lb)
            sys.exit(0 if result else 1)
        elif args.mode == 'deadline-fix':
            result = await test_deadline_fix(lb)
            sys.exit(0 if result else 1)
        elif args.mode == 'load-balance':
            result = await test_load_balancing(lb)
            sys.exit(0 if result else 1)
        elif args.mode == 'total-outage':
            result = await test_total_outage(lb)
            sys.exit(0 if result else 1)
        elif args.mode == 'continuous':
            await continuous_mode(lb)
        elif args.mode == 'all':
            results = {}
            results['load_balance'] = await test_load_balancing(lb)
            results['total_outage'] = await test_total_outage(lb)
            # Restore healthy addresses after total outage test
            async with lb._lock:
                lb.healthy_addresses = list(lb.all_addresses)
            await asyncio.sleep(3)
            
            logger.info('=== ALL TEST RESULTS ===')
            for name, passed in results.items():
                logger.info(f'  {name}: {"PASS" if passed else "FAIL"}')
    finally:
        await lb.close()


if __name__ == '__main__':
    asyncio.run(main())
