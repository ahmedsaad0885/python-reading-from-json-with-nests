import jaydebeapi
import pandas as pd
import json

# Connection details
host = "localhost"  # Replace with the actual IP address of the Denodo VM
port = "9999"  # Replace with the actual port if different
database = "denodo_training"
username = "admin"
password = "admin"
jar_file = "C:\\Users\\asmohamed\\Downloads\\denodo-vdp-jdbcdriver-8.0-update-20230914.jar"

# JDBC URL
jdbc_url = f"jdbc:vdb://{host}:{port}/{database}"

# Establish the connection
try:
    conn = jaydebeapi.connect(
        "com.denodo.vdb.jdbcdriver.VDBJDBCDriver",
        jdbc_url,
        [username, password],
        jar_file
    )
    print("connected")
except Exception as e:
    print(f"failed to connect: {e}")
    exit()

# SQL query
query = "SELECT * FROM bv_wo2_stock"#f_iv_product_details_array_flatten"

# Execute the query and fetch the data
def handle_jdbc_array(jdbc_array):
    """
    Converts a JDBC array to a Python list.
    """
    if jdbc_array is None:
        return None
    array = jdbc_array.getArray()
    return list(array) if array else None

def handle_jdbc_struct(jdbc_struct):
    """
    Converts a JDBC struct to a Python dictionary.
    """
    if jdbc_struct is None:
        return None
    attributes = jdbc_struct.getAttributes()
    attribute_names = jdbc_struct.getMetaData().getColumnNames()
    return {name: handle_complex_type(attr) for name, attr in zip(attribute_names, attributes)}

def handle_complex_type(val):
    """
    Handle complex types such as arrays and structs.
    """
    if hasattr(val, 'getArray'):  # Check for JDBC Array
        return handle_jdbc_array(val)
    elif hasattr(val, 'getAttributes'):  # Check for JDBC Struct
        return handle_jdbc_struct(val)
    return val

try:
    # Execute the query and fetch the data
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    # Convert rows to a list of dictionaries
    data_list = []
    for row in rows:
        row_dict = {}
        for col, val in zip(columns, row):
            row_dict[col] = handle_complex_type(val)
        data_list.append(row_dict)

    cursor.close()
except Exception as e:
    print(f"Query execution failed: {e}")
    conn.close()
    exit()

# Close the connection
conn.close()

# Write the data to a JSON file
output_file = 'customer_interactions.json'
try:
    with open(output_file, 'w') as json_file:
        json.dump(data_list, json_file, indent=4)
    print("Data successfully written to customer_interactions.json")
except Exception as e:
    print(f"Failed to write JSON: {e}")