import multiprocessing as mp
import os
from collections import defaultdict

INPUT_FILE = "testcase.txt"
OUTPUT_FILE = "output.txt"
NUM_WORKERS = max(1, os.cpu_count() - 1)  # Use all but one CPU core

def default_values():
    """Returns a default structure for city statistics."""
    return [None, 0.0, None, 0]  # [min, sum, max, count]

def process_chunk(lines):
    """Processes a chunk of lines and computes min, sum, max, count per city."""
    city_data = defaultdict(default_values)

    for line in lines:
        city, score = line.strip().split(";")
        score = float(score)

        if city_data[city][0] is None:  # First occurrence
            city_data[city] = [score, score, score, 1]
        else:
            city_data[city][0] = min(city_data[city][0], score)  # Min
            city_data[city][1] += score  # Sum
            city_data[city][2] = max(city_data[city][2], score)  # Max
            city_data[city][3] += 1  # Count

    return dict(city_data)  # Convert to regular dict for pickling

def merge_results(results):
    """Merges results from multiple processes into a single dictionary."""
    final_data = defaultdict(default_values)

    for city_data in results:
        for city, (min_val, total, max_val, count) in city_data.items():
            if final_data[city][0] is None:  # First occurrence
                final_data[city] = [min_val, total, max_val, count]
            else:
                final_data[city][0] = min(final_data[city][0], min_val)
                final_data[city][1] += total
                final_data[city][2] = max(final_data[city][2], max_val)
                final_data[city][3] += count

    return final_data

def write_output(final_data):
    """Writes the sorted output to the output.txt file."""
    with open(OUTPUT_FILE, "w") as f:
        for city in sorted(final_data.keys(), key=str.lower):
            min_val, total, max_val, count = final_data[city]
            mean_val = round(total / count, 1)  # IEEE 754 round-to-infinity
            f.write(f"{city}={round(min_val, 1)}/{mean_val}/{round(max_val, 1)}\n")

def main():
    """Main function to handle file reading, processing, and writing."""
    with open(INPUT_FILE, "r") as f:
        lines = f.readlines()

    num_lines = len(lines)
    
    if num_lines < 100000:  # Use single-threaded mode for small inputs
        results = [process_chunk(lines)]
    else:
        chunk_size = max(1, num_lines // NUM_WORKERS)
        chunks = [lines[i:i + chunk_size] for i in range(0, num_lines, chunk_size)]

        with mp.Pool(processes=NUM_WORKERS) as pool:
            results = pool.map(process_chunk, chunks)

    final_data = merge_results(results)
    write_output(final_data)

if __name__ == "__main__":
    main()
