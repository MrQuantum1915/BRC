import multiprocessing as mp
import os
from itertools import islice
import math

def process_chunk(chunk):
    """Process a chunk of lines and return city statistics."""
    city_stats = {}
    for line in chunk:
        if not line.strip():
            continue
        try:
            city, temp_str = line.strip().split(';')
            temp = float(temp_str)
            if city not in city_stats:
                city_stats[city] = [temp, temp, temp, 1]  # [min, max, sum, count]
            else:
                stats = city_stats[city]
                stats[0] = min(stats[0], temp)  # min
                stats[1] = max(stats[1], temp)  # max
                stats[2] += temp  # sum
                stats[3] += 1  # count
        except (ValueError, IndexError):
            continue  # Skip invalid lines
    return city_stats

def merge_stats(global_stats, local_stats):
    """Merge local stats into global stats."""
    for city, stats in local_stats.items():
        if city not in global_stats:
            global_stats[city] = stats
        else:
            global_stats[city][0] = min(global_stats[city][0], stats[0])  # min
            global_stats[city][1] = max(global_stats[city][1], stats[1])  # max
            global_stats[city][2] += stats[2]  # sum
            global_stats[city][3] += stats[3]  # count

def chunk_reader(file_path, chunk_size):
    """Generator to read file in chunks."""
    with open(file_path, 'r') as f:
        while True:
            lines = list(islice(f, chunk_size))
            if not lines:
                break
            yield lines

def write_results(output_file, city_stats):
    """Write the final results to the output file."""
    with open(output_file, 'w') as f:
        for city in sorted(city_stats.keys()):
            stats = city_stats[city]
            min_temp = math.ceil(stats[0] * 10) / 10
            mean_temp = math.ceil((stats[2] / stats[3]) * 10) / 10
            max_temp = math.ceil(stats[1] * 10) / 10
            f.write(f"{city}={min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}\n")

def main():
    input_file = 'testcase.txt'
    output_file = 'output.txt'
    
    # Determine number of CPU cores to use
    num_processes = max(1, mp.cpu_count() - 1)
    
    # Get file size to determine chunk size
    file_size = os.path.getsize(input_file)
    chunk_size = max(100_000, min(1_000_000, file_size // (num_processes * 4)))
    
    # Create a pool of workers
    with mp.Pool(processes=num_processes) as pool:
        # Process chunks in parallel
        results = []
        for chunk in chunk_reader(input_file, chunk_size):
            results.append(pool.apply_async(process_chunk, (chunk,)))
        
        # Merge results incrementally
        global_stats = {}
        for result in results:
            local_stats = result.get()
            merge_stats(global_stats, local_stats)
    
    # Write the final results to the output file
    write_results(output_file, global_stats)

if __name__ == "__main__":
    main()