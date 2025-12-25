import teradatasql
import os

host = '192.168.137.129'
user = 'dbc'
pasw = 'dbc'

# Create a simple scalar function first to test Java UDF registration
sql = """
REPLACE FUNCTION ExportToTrino (
    TargetIPs VARCHAR(500),
    QueryID VARCHAR(100)
)
RETURNS INTEGER
LANGUAGE JAVA
NO SQL
PARAMETER STYLE JAVA
EXTERNAL NAME 'com.teradata.udf.export.ExportToTrino.triggerExport(java.lang.String,java.lang.String) returns int';
"""

try:
    with teradatasql.connect(host=host, user=user, password=pasw) as con:
        with con.cursor() as cur:
            cur.execute(sql)
    print("Success")
except Exception as e:
    print(f"Error: {e}")
