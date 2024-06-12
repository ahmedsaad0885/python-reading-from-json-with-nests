import json

# Path to the JSON file
input_file = 'customer_interactions.json'

# Read and print the JSON data
try:
    with open(input_file, 'r') as json_file:
        data = json.load(json_file)
        print(json.dumps(data, indent=4))
except Exception as e:
    print(f"Failed to read JSON: {e}")
