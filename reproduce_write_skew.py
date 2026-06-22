#!/usr/bin/env python3
"""Script to reproduce write skew anomaly in the distributed processing system.

This script sends many concurrent ProcessJob requests for the same batch_id
and verifies whether the batch threshold is violated (write skew).

Usage:
    python reproduce_write_skew.py [--workers ADDR1,ADDR2,...] [--threshold N] [--concurrent N]

Exit codes:
    0 - No write skew detected (count <= threshold)
    1 - Write skew detected (count > threshold)
"""

import asyncio
import argparse
import logging
import os
import sys
import uuid


import grpc
import asyncpg

sys.path.insert(0, "/app/generated")
sys.path.insert(0, "/app")

import worker_pb2
import worker_pb2_grpc

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("write-skew-test")


async def send_job(stub, job_id, batch_id, timeout=30.0):
    """Send a single ProcessJob request."""
    request = worker_pb2.JobRequest(
        job_id=job_id, batch_id=batch_id, payload=f"payload_{job_id}"
    )
    try:
        response = await stub.ProcessJob(request, timeout=timeout)
        logger.debug(f"Job {job_id} completed: {response.status}")
        return True
    except grpc.aio.AioRpcError as e:
        logger.debug(f"Job {job_id} failed: {e.code()}")
        return False
    except Exception as e:
        logger.debug(f"Job {job_id} error: {e}")
        return False


async def main():
    parser = argparse.ArgumentParser(description="Reproduce Write Skew Anomaly")
    parser.add_argument(
        "--workers",
        default=os.environ.get(
            "WORKER_ADDRESSES", "worker-1:50051,worker-2:50051,worker-3:50051"
        ),
        help="Comma-separated worker addresses",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=int(os.environ.get("BATCH_THRESHOLD", "10")),
        help="Batch threshold (max allowed results per batch)",
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=30,
        help="Number of concurrent requests to send",
    )
    parser.add_argument(
        "--timeout", type=float, default=30.0, help="Timeout per request in seconds"
    )

    args = parser.parse_args()

    addresses = [addr.strip() for addr in args.workers.split(",")]
    batch_id = f"skew-test-{uuid.uuid4().hex[:8]}"

    logger.info(f"=== Write Skew Reproduction Test ===")
    logger.info(f"Workers: {addresses}")
    logger.info(f"Batch ID: {batch_id}")
    logger.info(f"Threshold: {args.threshold}")
    logger.info(f"Concurrent requests: {args.concurrent}")

    # Clear any existing data for this batch
    try:
        conn = await asyncpg.connect(
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
            database=os.environ.get("POSTGRES_DB", "loadbalancer"),
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
        )
        await conn.execute("DELETE FROM results WHERE batch_id = $1", batch_id)
        await conn.close()
    except Exception as e:
        logger.warning(f"Could not clear batch data: {e}")

    # Create stubs for all workers (round-robin)
    channels = []
    stubs = []
    for addr in addresses:
        channel = grpc.aio.insecure_channel(addr)
        channels.append(channel)
        stubs.append(worker_pb2_grpc.WorkerServiceStub(channel))

    # Send concurrent requests
    tasks = []
    for i in range(args.concurrent):
        job_id = f"{batch_id}-job-{i}"
        stub = stubs[i % len(stubs)]  # Round-robin
        tasks.append(send_job(stub, job_id, batch_id, timeout=args.timeout))

    logger.info(f"Sending {args.concurrent} concurrent requests...")
    results = await asyncio.gather(*tasks)

    successes = sum(1 for r in results if r)
    failures = sum(1 for r in results if not r)
    logger.info(f"Results: {successes} successes, {failures} failures")

    # Close channels
    for ch in channels:
        await ch.close()

    # Check final count in database
    conn = await asyncpg.connect(
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
        database=os.environ.get("POSTGRES_DB", "loadbalancer"),
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
    )

    final_count = await conn.fetchval(
        "SELECT COUNT(*) FROM results WHERE batch_id = $1", batch_id
    )
    await conn.close()

    logger.info(f"\n=== RESULTS ===")
    logger.info(f"Final count for batch {batch_id}: {final_count}")
    logger.info(f"Threshold: {args.threshold}")

    if final_count > args.threshold:
        logger.error(
            f"WRITE SKEW DETECTED! Count ({final_count}) exceeds threshold ({args.threshold})"
        )
        logger.error(
            f"This proves the write skew anomaly — {final_count - args.threshold} extra rows!"
        )
        sys.exit(1)
    else:
        logger.info(
            f"No write skew: count ({final_count}) is within threshold ({args.threshold})"
        )
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
