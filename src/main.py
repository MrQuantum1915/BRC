import multiprocessing as mp
import os
from collections import defaultdict
from itertools import islice

def process_chunk(chunk):
    
    city_stats = {}
    
    for line in chunk:
        if not line.strip():
            continue
        
        try:
            city, temp_str = line.strip().split(';')
            temp = float(temp_str)
            
            if city not in city_stats:
                
                city_stats[city] = [temp, temp, 1, temp]
            else:
                stats = city_stats[city]
                stats[0] = min(stats[0], temp)  
                stats[1] += temp  
                stats[2] += 1  
                stats[3] = max(stats[3], temp)  
        except Exception:
            
            continue
    
    return dict(city_stats)  

def merge_results(results):
    
    merged = {}
    
    for city_stats in results:
        for city, stats in city_stats.items():
            if city not in merged:
                
                merged[city] = stats.copy()
            else:
                merged_stats = merged[city]
                merged_stats[0] = min(merged_stats[0], stats[0])  
                merged_stats[1] += stats[1]  
                merged_stats[2] += stats[2]  
                merged_stats[3] = max(merged_stats[3], stats[3])  
    
    return merged

def read_and_process_file(input_file, num_processes):
    
    with mp.Manager() as manager:
        
        file_size = os.path.getsize(input_file)
        
        
        if file_size < 10_000_000:  
            with open(input_file, 'r') as f:
                lines = f.readlines()
            return process_chunk(lines)
        
        
        chunk_size = max(100_000, min(1_000_000, file_size // (num_processes * 4)))
        
        
        all_results = []
        with mp.Pool(processes=num_processes) as pool:
            with open(input_file, 'r') as f:
                while True:
                    chunk = list(islice(f, chunk_size))
                    if not chunk:
                        break
                    
                    
                    result = pool.apply_async(process_chunk, args=(chunk,))
                    all_results.append(result)
            
            
            results = [result.get() for result in all_results]
        
        
        return merge_results(results)

def write_results(stats, output_file):
    
    results = []
    
    for city, (min_temp, temp_sum, count, max_temp) in stats.items():
        mean_temp = temp_sum / count
        
        results.append(f"{city}={min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}")
    
    
    results.sort()
    
    
    with open(output_file, 'w') as f:
        f.write('\n'.join(results))

def main():
    input_file = 'testcase.txt'
    output_file = 'output.txt'
    
    
    num_cpu = max(1, mp.cpu_count() - 1)
    
    
    stats = read_and_process_file(input_file, num_cpu)
    
    
    write_results(stats, output_file)

if __name__ == "__main__":
    main()