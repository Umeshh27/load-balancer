import asyncio
import logging
import os
import signal
import sys

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

# Add generated code to path
sys.path.insert(0, '/app/generated')
sys.path.insert(0, '/app')

import enrichment_pb2
import enrichment_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('enrichment-service')

ENRICHMENT_LATENCY = float(os.environ.get('ENRICHMENT_LATENCY', '3.0'))


class EnrichmentServicer(enrichment_pb2_grpc.EnrichmentServiceServicer):
    async def Enrich(self, request, context):
        logger.info(f'Received Enrich request for job_id={request.job_id}')
        logger.info(f'Sleeping for {ENRICHMENT_LATENCY}s to simulate processing...')
        
        try:
            await asyncio.sleep(ENRICHMENT_LATENCY)
        except asyncio.CancelledError:
            logger.warning(f'Enrich request for job_id={request.job_id} was cancelled')
            context.set_code(grpc.StatusCode.CANCELLED)
            context.set_details('Request cancelled')
            return enrichment_pb2.EnrichResult()
        
        result = enrichment_pb2.EnrichResult(
            job_id=request.job_id,
            enriched_data=f'enriched_{request.raw_data}'
        )
        logger.info(f'Completed Enrich request for job_id={request.job_id}')
        return result


async def serve():
    server = grpc.aio.server()
    enrichment_pb2_grpc.add_EnrichmentServiceServicer_to_server(
        EnrichmentServicer(), server
    )
    
    # Add health checking
    health_servicer = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await health_servicer.set(
        '',
        health_pb2.HealthCheckResponse.SERVING
    )
    await health_servicer.set(
        'enrichment.EnrichmentService',
        health_pb2.HealthCheckResponse.SERVING
    )
    
    listen_addr = '[::]:50052'
    server.add_insecure_port(listen_addr)
    logger.info(f'Enrichment Service starting on {listen_addr}')
    logger.info(f'Configured latency: {ENRICHMENT_LATENCY}s')
    await server.start()
    
    async def shutdown(sig):
        logger.info(f'Received shutdown signal {sig}')
        await health_servicer.set(
            '',
            health_pb2.HealthCheckResponse.NOT_SERVING
        )
        await server.stop(5)
    
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
    
    logger.info('Enrichment Service is ready')
    await server.wait_for_termination()


if __name__ == '__main__':
    asyncio.run(serve())
