#!/usr/bin/env python3
"""Benchmark comparing pessimistic vs optimistic locking throughput.

Runs the write skew test at different concurrency levels and generates
LOCKING_COMPARISON.png bar chart.
"""

import asyncio
import argparse
import logging
import os
import sys
import time
import uuid

import grpc
import asyncpg
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, "/app/generated")
sys.path.insert(0, "/app")

import worker_pb2
import worker_pb2_grpc

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("benchmark")


async def send_job(stub, job_id, batch_id, timeout=60.0):
    request = worker_pb2.JobRequest(
        job_id=job_id, batch_id=batch_id, payload=f"benchmark_{job_id}"
    )
    try:
        await stub.ProcessJob(request, timeout=timeout)
        return True
    except Exception:
        return False


async def run_benchmark(addresses, concurrency, batch_prefix):
    """Run a benchmark with given concurrency and return throughput."""
    channels = []
    stubs = []
    for addr in addresses:
        channel = grpc.aio.insecure_channel(addr)
        channels.append(channel)
        stubs.append(worker_pb2_grpc.WorkerServiceStub(channel))

    batch_id = f"{batch_prefix}-{uuid.uuid4().hex[:8]}"

    # Clear data
    conn = await asyncpg.connect(
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
        database=os.environ.get("POSTGRES_DB", "loadbalancer"),
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
    )
    await conn.execute("DELETE FROM results WHERE batch_id LIKE $1", f"{batch_prefix}%")
    await conn.close()

    tasks = []
    for i in range(concurrency):
        job_id = f"{batch_id}-{i}"
        stub = stubs[i % len(stubs)]
        tasks.append(send_job(stub, job_id, batch_id))

    start = time.time()
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start

    for ch in channels:
        await ch.close()

    successes = sum(1 for r in results if r)
    throughput = successes / elapsed if elapsed > 0 else 0

    return {
        "concurrency": concurrency,
        "successes": successes,
        "elapsed": elapsed,
        "throughput": throughput,
    }


async def main():
    parser = argparse.ArgumentParser(description="Locking Benchmark")
    parser.add_argument(
        "--workers",
        default=os.environ.get(
            "WORKER_ADDRESSES", "worker-1:50051,worker-2:50051,worker-3:50051"
        ),
    )
    parser.add_argument("--output", default="/app/LOCKING_COMPARISON.png")
    args = parser.parse_args()

    addresses = [addr.strip() for addr in args.workers.split(",")]
    concurrency_levels = [10, 50, 100]

    # Note: This benchmark needs to be run twice - once with LOCKING_MODE=pessimistic
    # and once with LOCKING_MODE=optimistic on the workers.
    # The results are collected and plotted.

    logger.info("=== Locking Benchmark ===")
    logger.info("Running benchmark...")

    results = []
    for concurrency in concurrency_levels:
        logger.info(f"Testing concurrency={concurrency}...")
        result = await run_benchmark(addresses, concurrency, f"bench-{concurrency}")
        results.append(result)
        logger.info(
            f'  Throughput: {result["throughput"]:.2f} jobs/sec '
            f'({result["successes"]}/{concurrency} in {result["elapsed"]:.2f}s)'
        )
        await asyncio.sleep(1)  # Brief pause between tests

    return results


def generate_chart(pessimistic_results, optimistic_results, output_path):
    """Generate comparison bar chart."""
    concurrency_levels = [r["concurrency"] for r in pessimistic_results]
    pess_throughput = [r["throughput"] for r in pessimistic_results]
    opt_throughput = [r["throughput"] for r in optimistic_results]

    fig, ax = plt.subplots(figsize=(10, 6))

    x = range(len(concurrency_levels))
    width = 0.35

    bars1 = ax.bar(
        [i - width / 2 for i in x],
        pess_throughput,
        width,
        label="Pessimistic (SELECT FOR UPDATE)",
        color="#2196F3",
        alpha=0.8,
    )
    bars2 = ax.bar(
        [i + width / 2 for i in x],
        opt_throughput,
        width,
        label="Optimistic (Version Check)",
        color="#FF9800",
        alpha=0.8,
    )

    ax.set_xlabel("Concurrency Level", fontsize=12)
    ax.set_ylabel("Throughput (jobs/sec)", fontsize=12)
    ax.set_title(
        "Pessimistic vs Optimistic Locking: Throughput Comparison", fontsize=14
    )
    ax.set_xticks(x)
    ax.set_xticklabels([str(c) for c in concurrency_levels])
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    # Add value labels on bars
    for bar in bars1:
        height = bar.get_height()
        ax.annotate(
            f"{height:.1f}",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    for bar in bars2:
        height = bar.get_height()
        ax.annotate(
            f"{height:.1f}",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    logger.info(f"Chart saved to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
