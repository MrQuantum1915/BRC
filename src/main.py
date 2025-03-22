import multiprocessing as mp
import os
from itertools import islice
import math
def process_chunk(chunk):
    # Process a chunk of lines and return city statistics
    city_stats = {}
    
    for line in chunk:
        if not line.strip():
            continue
        
        try:
            parts = line.strip().split(';')
            if len(parts) != 2:
                continue
                
            city, temp_str = parts
            temp = float(temp_str)
            
            if city not in city_stats:
                # Initialize: [min, sum, count, max]
                city_stats[city] = [temp, temp, 1, temp]
            else:
                stats = city_stats[city]
                stats[0] = min(stats[0], temp)  # min
                stats[1] += temp  # sum
                stats[2] += 1  # count
                stats[3] = max(stats[3], temp)  # max
        except (ValueError, IndexError):
            # Skip invalid lines
            continue
    
    return city_stats

def chunk_reader(file_path, chunk_size):
    """Generator to read file in chunks"""
    with open(file_path, 'r') as f:
        while True:
            lines = list(islice(f, chunk_size))
            if not lines:
                break
            yield lines

def main():
    input_file = 'testcase.txt'
    output_file = 'output.txt'
    
    # Determine number of CPU cores to use
    num_processes = max(1, mp.cpu_count() - 1)
    
    # Get file size to determine chunk size
    file_size = os.path.getsize(input_file)
    chunk_size = max(100_000, min(1_000_000, file_size // (num_processes * 4)))
    
    # Create pool of workers
    with mp.Pool(processes=num_processes) as pool:
        # Submit tasks for each chunk
        results = []
        for chunk in chunk_reader(input_file, chunk_size):
            result = pool.apply_async(process_chunk, (chunk,))
            results.append(result)
        
        # Get results and merge
        merged_stats = {}
        for result in results:
            chunk_stats = result.get()
            for city, stats in chunk_stats.items():
                if city not in merged_stats:
                    merged_stats[city] = stats.copy()
                else:
                    merged_stats[city][0] = min(merged_stats[city][0], stats[0])  # min
                    merged_stats[city][1] += stats[1]  # sum
                    merged_stats[city][2] += stats[2]  # count
                    merged_stats[city][3] = max(merged_stats[city][3], stats[3])  # max
    
    # Format and write output with correct IEEE 754 rounding (ceiling to one decimal place)
    formatted_results = []
    for city, (min_temp, sum_temp, count, max_temp) in merged_stats.items():
        # Use ceil rounding for all values as per IEEE 754 "round to infinity" standard
        min_rounded = math.ceil(min_temp * 10) / 10
        mean_temp = sum_temp / count
        mean_rounded = math.ceil(mean_temp * 10) / 10
        max_rounded = math.ceil(max_temp * 10) / 10
        
        # Format with exactly one decimal place
        formatted_results.append(f"{city}={min_rounded:.1f}/{mean_rounded:.1f}/{max_rounded:.1f}")
    
    # Sort alphabetically
    formatted_results.sort()
    
    # Write results
    with open(output_file, 'w') as f:
        f.write('\n'.join(formatted_results))

if __name__ == "__main__":
    main()