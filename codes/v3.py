import math
from collections import defaultdict

def process_file(input_file_name="testcase.txt", output_file_name="output.txt"):
    values = defaultdict(lambda: [float('inf'), float('-inf'), 0, 0])

    with open(input_file_name, "r") as input_file:
        for line in input_file:
            key, value = line.strip().split(';')  
            value = float(value) 

            values[key][0] = min(values[key][0], value) 
            values[key][1] = max(values[key][1], value) 
            values[key][2] += value                     
            values[key][3] += 1                         

    with open(output_file_name, "w") as output_file:
        for key in sorted(values.keys()):
            minX = math.ceil(values[key][0] * 10) / 10
            meanX = math.ceil((values[key][2] / values[key][3]) * 10) / 10 if values[key][3] > 0 else 0
            maxX = math.ceil(values[key][1] * 10) / 10 
            output_file.write(f"{key}={minX}/{meanX}/{maxX}\n")

if __name__ == "__main__":
    process_file("testcase.txt", "output.txt")