#!/usr/bin/env python3

import csv
import json
import re
from pathlib import Path

import matplotlib.pyplot as plt


# =============================================================================
# Configuration
# =============================================================================

INTERMEDIATE_DIR = Path("logs/intermediate")

CACHE_FILE = Path("logs/benchmark_cache.json")
COMBINED_FILE = Path("logs/combined_results.csv")
GRAPH_FILE = Path("logs/throughput_vs_concurrency.png")

# Matches files such as:
#
#   client_A_64conc.csv
#   client_0_128conc.csv
#   client_whatever_256conc.csv
#
FILE_PATTERN = re.compile(r"client_.+_(\d+)conc\.csv$")


# =============================================================================
# Cache
# =============================================================================

def load_cache():
    if not CACHE_FILE.exists():
        return {
            "files": {}
        }

    try:
        with CACHE_FILE.open("r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        print("Cache is invalid. Rebuilding it.")
        return {
            "files": {}
        }


def save_cache(cache):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)

    with CACHE_FILE.open("w") as f:
        json.dump(cache, f, indent=2)


# =============================================================================
# CSV Parsing
# =============================================================================

def read_result_file(path: Path):
    """
    Reads one client benchmark CSV.

    Expected columns:

        run_duration_s
        concurrency
        generated_requests
        completed_requests
        completed_within_run
        request_throughput_rps
        message_throughput_mps
    """

    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        row = next(reader, None)

    if row is None:
        raise ValueError(f"{path} contains no data")

    return {
        "concurrency": int(row["concurrency"]),
        "run_duration_s": float(row["run_duration_s"]),
        "generated_requests": int(row["generated_requests"]),
        "completed_requests": int(row["completed_requests"]),
        "completed_within_run": int(row["completed_within_run"]),
        "request_throughput_rps": float(row["request_throughput_rps"]),
        "message_throughput_mps": float(row["message_throughput_mps"]),
    }


def get_file_result(path: Path, cache):
    """
    Returns parsed benchmark information.

    If the file has not changed since the last execution,
    return the cached version instead of reopening/parsing the CSV.
    """

    stat = path.stat()

    fingerprint = {
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }

    cache_key = str(path.resolve())

    cached = cache["files"].get(cache_key)

    if cached is not None:
        if (
            cached.get("size") == fingerprint["size"]
            and cached.get("mtime_ns") == fingerprint["mtime_ns"]
        ):
            return cached["result"], True

    result = read_result_file(path)

    cache["files"][cache_key] = {
        **fingerprint,
        "result": result,
    }

    return result, False


# =============================================================================
# Aggregation
# =============================================================================

def collect_results():
    cache = load_cache()

    grouped = {}

    files = sorted(INTERMEDIATE_DIR.glob("client_*_*conc.csv"))

    if not files:
        raise RuntimeError(
            f"No benchmark CSV files found in {INTERMEDIATE_DIR}"
        )

    used_cache = 0
    parsed = 0

    valid_paths = set()

    for path in files:
        if not FILE_PATTERN.match(path.name):
            continue

        valid_paths.add(str(path.resolve()))

        try:
            result, was_cached = get_file_result(path, cache)
        except Exception as e:
            print(f"Skipping {path}: {e}")
            continue

        if was_cached:
            used_cache += 1
        else:
            parsed += 1

        concurrency = result["concurrency"]

        grouped.setdefault(concurrency, []).append(result)

    # Remove cache entries for files that no longer exist.
    stale_entries = [
        key
        for key in cache["files"]
        if key not in valid_paths
    ]

    for key in stale_entries:
        del cache["files"][key]

    save_cache(cache)

    print(
        f"Input CSVs: {used_cache + parsed} "
        f"({used_cache} cached, {parsed} parsed)"
    )

    return grouped


def aggregate_results(grouped):
    """
    All client actors running with the same configured concurrency
    are treated as one benchmark point.

    Example:

        10 client actors
        concurrency = 64 each

    Their throughput values are summed to obtain aggregate system
    throughput for the concurrency=64 experiment.
    """

    combined = []

    for concurrency in sorted(grouped):
        runs = grouped[concurrency]

        client_count = len(runs)

        run_durations = {
            result["run_duration_s"]
            for result in runs
        }

        if len(run_durations) != 1:
            print(
                f"WARNING: concurrency={concurrency} has "
                f"different run durations: {sorted(run_durations)}"
            )

        aggregate_generated = sum(
            result["generated_requests"]
            for result in runs
        )

        aggregate_completed = sum(
            result["completed_requests"]
            for result in runs
        )

        aggregate_completed_within_run = sum(
            result["completed_within_run"]
            for result in runs
        )

        aggregate_request_throughput = sum(
            result["request_throughput_rps"]
            for result in runs
        )

        aggregate_message_throughput = sum(
            result["message_throughput_mps"]
            for result in runs
        )

        combined.append({
            "concurrency": concurrency,
            "client_count": client_count,

            # Useful if you later care about actual total number of
            # simultaneously outstanding requests across all clients.
            "total_concurrency": concurrency * client_count,

            "generated_requests": aggregate_generated,
            "completed_requests": aggregate_completed,
            "completed_within_run": aggregate_completed_within_run,

            "request_throughput_rps": aggregate_request_throughput,
            "message_throughput_mps": aggregate_message_throughput,
        })

    return combined


# =============================================================================
# Output combined CSV
# =============================================================================

def write_combined_csv(results):
    COMBINED_FILE.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "concurrency",
        "client_count",
        "total_concurrency",
        "generated_requests",
        "completed_requests",
        "completed_within_run",
        "request_throughput_rps",
        "message_throughput_mps",
    ]

    with COMBINED_FILE.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(results)

    print(f"Combined results: {COMBINED_FILE}")


# =============================================================================
# Plot
# =============================================================================

def plot_results(results):
    if not results:
        raise RuntimeError("No benchmark results to plot")

    concurrency = [
        result["concurrency"]
        for result in results
    ]

    throughput = [
        result["request_throughput_rps"]
        for result in results
    ]

    plt.figure(figsize=(9, 6))

    plt.plot(
        concurrency,
        throughput,
        marker="o",
        linewidth=2,
    )

    plt.xlabel("Concurrency")
    plt.ylabel("Aggregate throughput (requests/sec)")
    plt.title("Reactor Throughput vs Concurrency")

    plt.grid(
        True,
        linestyle="--",
        alpha=0.4,
    )

    plt.ticklabel_format(
        axis="y",
        style="plain",
    )

    plt.tight_layout()

    GRAPH_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    plt.savefig(
        GRAPH_FILE,
        dpi=200,
    )

    print(f"Graph: {GRAPH_FILE}")

    plt.show()


# =============================================================================
# Main
# =============================================================================

def main():
    grouped = collect_results()

    combined = aggregate_results(grouped)

    print()
    print("Aggregated benchmark results")
    print("-" * 80)

    for result in combined:
        print(
            f"concurrency={result['concurrency']:>6} | "
            f"clients={result['client_count']:>3} | "
            f"throughput={result['request_throughput_rps']:>15,.2f} req/s | "
            f"messages={result['message_throughput_mps']:>15,.2f} msg/s"
        )

    print()

    write_combined_csv(combined)
    plot_results(combined)


if __name__ == "__main__":
    main()
