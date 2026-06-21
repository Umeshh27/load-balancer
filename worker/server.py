import asyncio
import logging
import os
import signal
import sys
import time
import uuid

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

sys.path.insert(0, '/app/generated')
sys.path.insert(0, '/app')

import worker_pb2
import worker_pb2_grpc
import enrichment_pb2
import enrichment_pb2_grpc
import db

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('worker')

WORKER_ID = os.environ.get('WORKER_ID', f'worker-{uuid.uuid4().hex[:8]}')
DEADLINE_PROPAGATION = os.environ.get('DEADLINE_PROPAGATION', 'true').lower() == 'true'
LOCKING_MODE = os.environ.get('LOCKING_MODE', 'none')
ENRICHMENT_HOST = os.environ.get('ENRICHMENT_HOST', 'enrichment-service')
ENRICHMENT_PORT = os.environ.get('ENRICHMENT_PORT', '50052')

logger.info(f'Worker config: ID={WORKER_ID}, DEADLINE_PROPAGATION={DEADLINE_PROPAGATION}, '
            f'LOCKING_MODE={LOCKING_MODE}')


class WorkerServicer(worker_pb2_grpc.WorkerServiceServicer):
    def __init__(self):
        self.enrichment_channel = None
        self.enrichment_stub = None
    
    async def _get_enrichment_stub(self):
        if self.enrichment_stub is None:
            addr = f'{ENRICHMENT_HOST}:{ENRICHMENT_PORT}'
            self.enrichment_channel = grpc.aio.insecure_channel(addr)
            self.enrichment_stub = enrichment_pb2_grpc.EnrichmentServiceStub(
                self.enrichment_channel
            )
            logger.info(f'Connected to enrichment service at {addr}')
        return self.enrichment_stub
    
    async def _do_process_wasted(self, request, stub, enrich_request):
        enrich_response = await stub.Enrich(enrich_request)
        logger.info(f'[{WORKER_ID}] Enrichment completed for '
                   f'job_id={request.job_id}. Writing to DB...')
        
        result_data = enrich_response.enriched_data
        if request.batch_id:
            await db.insert_result(
                request.job_id, request.batch_id, result_data,
                WORKER_ID, locking_mode=LOCKING_MODE
            )
        else:
            await db.simple_insert(
                request.job_id, request.batch_id or 'default',
                result_data, WORKER_ID
            )
        logger.info(f'[{WORKER_ID}] DB write completed for '
                   f'job_id={request.job_id} (client may have already timed out!)')
        return result_data

    async def ProcessJob(self, request, context):
        logger.info(f'[{WORKER_ID}] Received ProcessJob: job_id={request.job_id}, '
                    f'batch_id={request.batch_id}')
        
        try:
            # Step 1: Call enrichment service
            stub = await self._get_enrichment_stub()
            enrich_request = enrichment_pb2.EnrichRequest(
                job_id=request.job_id,
                raw_data=request.payload
            )
            
            if DEADLINE_PROPAGATION:
                # Get remaining time from the incoming deadline
                remaining = context.time_remaining()
                logger.info(f'[{WORKER_ID}] Deadline propagation ON. '
                           f'Remaining time: {remaining:.2f}s')
                
                if remaining <= 0:
                    logger.warning(f'[{WORKER_ID}] Deadline already expired for '
                                 f'job_id={request.job_id}')
                    context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
                    context.set_details('Deadline already expired')
                    return worker_pb2.JobResult()
                
                try:
                    enrich_response = await stub.Enrich(
                        enrich_request,
                        timeout=remaining
                    )
                except grpc.aio.AioRpcError as e:
                    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                        logger.warning(f'[{WORKER_ID}] Enrichment call timed out for '
                                     f'job_id={request.job_id}. NOT writing to DB.')
                        context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
                        context.set_details('Downstream enrichment timed out')
                        return worker_pb2.JobResult()
                    elif e.code() == grpc.StatusCode.CANCELLED:
                        logger.warning(f'[{WORKER_ID}] Enrichment call cancelled for '
                                     f'job_id={request.job_id}. NOT writing to DB.')
                        return worker_pb2.JobResult()
                    raise
                except asyncio.CancelledError:
                    logger.warning(f'[{WORKER_ID}] Request cancelled for '
                                 f'job_id={request.job_id}. NOT writing to DB.')
                    return worker_pb2.JobResult()
                
                # Check if context is still active before DB write
                if context.cancelled():
                    logger.warning(f'[{WORKER_ID}] Context cancelled after enrichment '
                                 f'for job_id={request.job_id}. NOT writing to DB.')
                    return worker_pb2.JobResult()
                
                # Step 2: Write to database with remaining deadline
                remaining_after_enrich = context.time_remaining()
                if remaining_after_enrich <= 0:
                    logger.warning(f'[{WORKER_ID}] Deadline expired after enrichment '
                                 f'for job_id={request.job_id}. NOT writing to DB.')
                    context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
                    return worker_pb2.JobResult()
                
                try:
                    result_data = enrich_response.enriched_data
                    if request.batch_id:
                        await db.insert_result(
                            request.job_id, request.batch_id, result_data, 
                            WORKER_ID, locking_mode=LOCKING_MODE,
                            timeout=remaining_after_enrich
                        )
                    else:
                        await db.simple_insert(
                            request.job_id, request.batch_id or 'default',
                            result_data, WORKER_ID,
                            timeout=remaining_after_enrich
                        )
                except asyncio.TimeoutError:
                    logger.warning(f'[{WORKER_ID}] DB write timed out for '
                                 f'job_id={request.job_id}')
                    context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
                    return worker_pb2.JobResult()
                
            else:
                # NO deadline propagation — demonstrates wasted work
                logger.info(f'[{WORKER_ID}] Deadline propagation OFF. '
                           f'Processing without timeout forwarding (SHIELDED).')
                
                # Shield the actual work from client-side cancellation/timeout!
                result_data = await asyncio.shield(
                    self._do_process_wasted(request, stub, enrich_request)
                )
            
            return worker_pb2.JobResult(
                job_id=request.job_id,
                batch_id=request.batch_id,
                status='completed',
                result_data=result_data if 'result_data' in locals() else '',
                worker_id=WORKER_ID
            )
            
        except asyncio.CancelledError:
            logger.warning(f'[{WORKER_ID}] CancelledError for job_id={request.job_id}')
            return worker_pb2.JobResult()
        except grpc.aio.AioRpcError as e:
            logger.error(f'[{WORKER_ID}] gRPC error: {e.code()} - {e.details()}')
            context.set_code(e.code())
            context.set_details(str(e.details()))
            return worker_pb2.JobResult()
        except Exception as e:
            logger.error(f'[{WORKER_ID}] Unexpected error: {e}', exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return worker_pb2.JobResult()
    
    async def ProcessBatch(self, request_iterator, context):
        """Stream processing: receives stream of JobRequests, returns stream of JobResults."""
        async for request in request_iterator:
            logger.info(f'[{WORKER_ID}] ProcessBatch: processing job_id={request.job_id}')
            result = await self.ProcessJob(request, context)
            yield result


async def serve():
    # Initialize database
    await db.init_db()
    
    server = grpc.aio.server()
    servicer = WorkerServicer()
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)
    
    # Health checking
    health_servicer = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await health_servicer.set('', health_pb2.HealthCheckResponse.SERVING)
    await health_servicer.set(
        'worker.WorkerService',
        health_pb2.HealthCheckResponse.SERVING
    )
    
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    logger.info(f'[{WORKER_ID}] Worker starting on {listen_addr}')
    await server.start()
    
    async def shutdown(sig):
        logger.info(f'[{WORKER_ID}] Received shutdown signal {sig}')
        await health_servicer.set('', health_pb2.HealthCheckResponse.NOT_SERVING)
        await server.stop(5)
    
    loop = asyncio.get_event_loop()
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
    except NotImplementedError:
        pass  # Windows doesn't support add_signal_handler
    
    logger.info(f'[{WORKER_ID}] Worker is ready (DEADLINE_PROPAGATION={DEADLINE_PROPAGATION}, '
                f'LOCKING_MODE={LOCKING_MODE})')
    await server.wait_for_termination()


if __name__ == '__main__':
    asyncio.run(serve())
