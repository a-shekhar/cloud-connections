from snowflake.snowpark import Session
from ConnectionProperties import ConnectionProperties


conn = ConnectionProperties()
connection_parameters =  conn.getconnection()

# create the connection 
session = Session.builder.configs(connection_parameters).create()

"""
To save the contents of a DataFrame to a table:
Call the write property to get a DataFrameWriter object.
Call the mode method in the DataFrameWriter object and specify whether you want to insert rows or update rows in the table. This method returns a new DataFrameWriter object that is configured with the specified mode.
Call the save_as_table method in the DataFrameWriter object to save the contents of the DataFrame to a specified table.
Note that you do not need to call a separate method (e.g. collect) to execute the SQL statement that saves the data to the table.
"""

df = session.create_dataframe([1, 2, 3], schema=["col1"])
df.write.mode("overwrite").saveAsTable("createtabletest1")

df = session.table("createtabletest1")
df.show()

database = session.get_current_database()
schema = session.get_current_schema()
view_name = "myview1"
"""
Alternatively, use the create_or_replace_temp_view method, which creates a temporary view. 
The temporary view is only available in the session in which it is created.
"""
df.create_or_replace_view(f"{database}.{schema}.{view_name}")
print("aditya")
session.sql(f"SELECT col1 FROM {database}.{schema}.{view_name}")

