import teradatasql
import os

host = '192.168.137.129'
user = 'dbc'
password = 'dbc'

# Path to the source file
cpp_file_path = '/home/vijay/tdconnector/src/teradata/teradata_export.cpp'

with open(cpp_file_path, 'r') as f:
    cpp_source = f.read()

# Registration SQL
# We use 'CS' to send the source code for compilation on the Teradata node
# The format is 'CS!function_name!source_file_path_on_node!F!entry_point'
# But for Table Operators, we often just send the source directly or put it in a specific folder.
# More robust way for remote: send the source as part of the statement if possible, 
# or use the file-based approach if we have access.
# Since we are remote, we'll try to use the multi-file source approach if needed, 
# but for a single file, replacing the external name with the source is sometimes tricky.

register_sql = """
REPLACE FUNCTION ExportToTrino (
    TargetIPs VARCHAR(1000),
    QueryID VARCHAR(100)
)
RETURNS TABLE VARYING COLUMNS
LANGUAGE CPP
PARAMETER STYLE SQL_TABLE
EXTERNAL NAME 'CS!ExportToTrino!src/teradata/teradata_export.cpp!F!ExportToTrino';
"""

# Wait, the EXTERNAL NAME 'CS!...' usually expects the file to be present on the client machine 
# when using BTEQ with 'COMPILE'. With teradatasql, we might need a different approach 
# or ensure the file is where it expects.

print(f"Connecting to Teradata at {host}...")
try:
    with teradatasql.connect(host=host, user=user, password=password) as connect:
        with connect.cursor() as cursor:
            print("Successfully connected. Registering Table Operator...")
            # Note: teradatasql doesn't support the 'COMPILE' command like BTEQ.
            # We might need to use 'CREATE FUNCTION ... EXTERNAL NAME' and let TD find the source.
            # If the source is not on the server, we might need to use a client-side tool.
            # However, we'll try the direct SQL approach.
            cursor.execute(register_sql)
            print("Registration command sent.")
except Exception as e:
    print(f"Error: {e}")
