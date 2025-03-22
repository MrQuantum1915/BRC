import multiprocessing as mp
import os
import sys
from collections import defaultdict

INPUT_FILE = "testcase.txt"
OUTPUT_FILE = "output.txt"
NUM_WORKERS = max(1, os.cpu_count() - 1)  # Use all but one CPU core
def default_values():
    return [float("inf"), 0, float("-inf"), 0]


def process_chunk(lines):
    """Process a chunk of input lines."""
    # city_data = defaultdict(lambda: [float("inf"), 0, float("-inf"), 0])  # [min, sum, max, count]
    city_data = defaultdict(default_values)  # ✅ Picklable function

    for line in lines:
        city, score = line.strip().split(";")
        score = float(score)

        city_data[city][0] = min(city_data[city][0], score)  # Update min
        city_data[city][1] += score                           # Sum
        city_data[city][2] = max(city_data[city][2], score)  # Update max
        city_data[city][3] += 1                              # Count

    return city_data

def merge_results(results):
    """Merge results from all worker processes."""
    final_data = defaultdict(lambda: [float("inf"), 0, float("-inf"), 0])

    for city_data in results:
        for city, (min_val, total, max_val, count) in city_data.items():
            final_data[city][0] = min(final_data[city][0], min_val)
            final_data[city][1] += total
            final_data[city][2] = max(final_data[city][2], max_val)
            final_data[city][3] += count

    return final_data

def write_output(final_data):
    """Write the final results to output.txt in sorted order."""
    with open(OUTPUT_FILE, "w") as f:
        for city in sorted(final_data.keys(), key=str.lower):
            min_val, total, max_val, count = final_data[city]
            mean_val = round(total / count, 1)
            f.write(f"{city}={round(min_val, 1)}/{mean_val}/{round(max_val, 1)}\n")

def main():
    """Main function to orchestrate parallel processing."""
    with open(INPUT_FILE, "r") as f:
        lines = f.readlines()

    chunk_size = max(1, len(lines) // NUM_WORKERS)  
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    with mp.Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(process_chunk, chunks)

    final_data = merge_results(results)
    write_output(final_data)

if __name__ == "__main__":
    main()
