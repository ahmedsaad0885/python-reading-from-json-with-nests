import jaydebeapi
import json
import jpype

host = "localhost"
port = "9999"
database = "denodo_training"
username = "admin"
password = "admin"
jar_file = "C:\\Users\\asmohamed\\Downloads\\denodo-vdp-jdbcdriver-8.0-update-20230914.jar"

jdbc_url = f"jdbc:vdb://{host}:{port}/{database}"


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
query = "SELECT * FROM f_iv_product_details_array_flatten"# bv_wo2_stock # iv_customer_address # f_iv_product_details_array_flatten # iv_customer_interactions2 # iv_current_customer

# Handle BigDecimal conversion
def handle_big_decimal(val):
    """
    #Converts java.math.BigDecimal to float.
    """
    BigDecimal = jpype.JClass("java.math.BigDecimal")
    if isinstance(val, BigDecimal):
        return float(val.doubleValue())
    return val

# Handle Timestamp conversion
def handle_timestamp(val):
    """
    Converts java.sql.Timestamp to string.
    """
    Timestamp = jpype.JClass("java.sql.Timestamp")
    if isinstance(val, Timestamp):
        return val.toString()#todo better handeling for timestamps
    return val

# Handling complex data types
def handle_jdbc_array(jdbc_array):
    """
    Converts a JDBC array to a Python list.
    """
    if jdbc_array is None:
        return None
    array = jdbc_array.getArray()#retrive the jdbc array
    if array is None:#for  arrays with empty values
        return None

    #convert to python list
    array_list = []
    for item in array:
        array_list.append(handle_complex_type(item))
    return array_list

def handle_jdbc_struct(jdbc_struct):
    """
    Converts a JDBC struct to a Python dictionary.
    """
    if jdbc_struct is None:
        return None
    attributes = jdbc_struct.getAttributes() #extracts the attributes of the JDBC struct. Each attribute is an element of the struct
    return {f"attr_{i}": handle_complex_type(attr) for i, attr in enumerate(attributes)} #converts the attributes into a dictionary where each key is attr_i and the value is processed through handle_complex_type

def handle_complex_type(val):
    """
    Handle complex types such as arrays, structs, BigDecimal, and Timestamp.
    """
    if hasattr(val, 'getArray'):  # Check for JDBC Array
        return handle_jdbc_array(val)
    elif hasattr(val, 'getAttributes'):  # Check for JDBC Struct
        return handle_jdbc_struct(val)
    elif hasattr(val, 'doubleValue'):  # Check for BigDecimal
        return handle_big_decimal(val)
    elif hasattr(val, 'toString'):  # Check for Timestamp
        return handle_timestamp(val)
    return val #if no complex data_type return the value as is

try:
    # Execute the query and fetch the data
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]#After executing the query, the cursor’s description attribute contains metadata about the columns.
    rows = cursor.fetchall()#The fetchall() method retrieves all rows returned by the query and stores them in the rows list.TODO make better performance

    # Convert rows to a list of dictionaries
    data_list = []
    for row in rows:
        row_dict = {}
        for col, val in zip(columns, row):#pairs each column name with the corresponding value
            row_dict[col] = handle_complex_type(val)#Each value (val) is processed by the handle_complex_type
        data_list.append(row_dict)

    cursor.close()
except Exception as e:
    print(f"Query execution failed: {e}")
    conn.close()
    exit()


conn.close()

# Write the data to a JSON file
output_file = 'customer_interactions2.json'
try:
    with open(output_file, 'w') as json_file:
        json.dump(data_list, json_file, indent=4)
    print("Data successfully written to customer_interactions2.json")
except Exception as e:
    print(f"Failed to write JSON: {e}")
