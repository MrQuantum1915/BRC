# import math

# def main(input_file_name="testcase.txt", output_file_name="output.txt"):
#     input_file = open(input_file_name, "r")
#     output_file = open(output_file_name, "w")

#     minmap = {}
#     maxmap = {}
#     meanmap = {}

#     for line in input_file:
#         li = line.strip().split(';')
#         key, value = li[0], float(li[1])
#         if key in minmap:
#             minmap[key] = min(minmap[key], value)
#         else:
#             minmap[key] = value
#         if key in maxmap:
#             maxmap[key] = max(maxmap[key], value)
#         else:
#             maxmap[key] = value
#         if key in meanmap:
#             meanmap[key][0] += value
#             meanmap[key][1] += 1
#         else:
#             meanmap[key] = [value, 1]

#     for key in sorted(minmap.keys()):
#         minX = math.ceil(minmap[key] * 10) / 10
#         meanX = math.ceil((meanmap[key][0]/meanmap[key][1]) * 10) / 10
#         maxX = math.ceil(maxmap[key] * 10) / 10
#         output_file.write(f"{key}={minX}/{meanX}/{maxX}\n")

#     output_file.close()
#     input_file.close()


# if __name__ == "__main__":
#     main()




import math

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    
    values = {}

    with open(input_file_name, "r") as input_file:
        for line in input_file:
            key, value = line.strip().split(';')
            value = float(value)

            if key not in values:
                values[key] = [value, value, value, 1]
            else:
                values[key][0] = min(values[key][0], value)
                values[key][1] = max(values[key][1], value)
                values[key][2] += value
                values[key][3] += 1

    with open(output_file_name, "w") as output_file:
        for key in sorted(values.keys()):
            minX = math.ceil(values[key][0] * 10) / 10
            meanX = math.ceil((values[key][2] / values[key][3]) * 10) / 10
            maxX = math.ceil(values[key][1] * 10) / 10
            output_file.write(f"{key}={minX}/{meanX}/{maxX}\n")

if __name__ == "__main__":
    main()