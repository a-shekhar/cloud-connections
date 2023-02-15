from snowflake.snowpark import Session
from ConnectionProperties import ConnectionProperties
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException

conn = ConnectionProperties()
connection_parameters =  conn.getconnection()

# create the connection 
session = Session.builder.configs(connection_parameters).create()

"""
Because each method that transforms a DataFrame object returns a new DataFrame object that has the transformation applied,
you can chain method calls to produce a new DataFrame that is transformed in additional ways.
"""

chain1 = session.table("sample_product_data").filter(col("id") == 1).select(col("name"), col("serial_number"))
chain1.show()

"""When you chain method calls, keep in mind that the order of calls is important. 
Each method call returns a DataFrame that has been transformed. Make sure that subsequent calls work with the transformed DataFrame.
in below example we should get excpetion as we don't have "ID" column returned by select statement to use filter on

I don't know why we are not getting exception, will research
"""
chain2 = session.table("sample_product_data").select(col("name"), col("serial_number")).filter(col("id") == 1)
try:
    chain2.show()
except SnowparkSQLException as e:
    print(e.message)

