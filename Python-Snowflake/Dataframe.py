from snowflake.snowpark import Session
from snowflake.snowpark import Row
from snowflake.snowpark.types import  IntegerType, StringType, StructType, StructField
from ConnectionProperties import ConnectionProperties

conn = ConnectionProperties()
connection_parameters = conn.getconnection()

# create the connection 
session = Session.builder.configs(connection_parameters).create()

#create a DataFrame from specified values
df1 = session.create_dataframe([1, 2, 3, 4]).to_df("a")
df1.show()

# Create a DataFrame with 4 columns, "a", "b", "c" and "d".
df2 = session.create_dataframe([[1, 2, 3, 4]], schema = (['a','b', 'c', 'd']))
df2.show()

# Create another DataFrame with 4 columns, "a", "b", "c" and "d".
df3 = session.create_dataframe([Row(a = 1, b = 2, c = 3, d = 4)])
df3.show()

# Create a DataFrame and specify a schema
schema =  StructType([StructField("a", IntegerType()), StructField("b", StringType())])
df4 = session.create_dataframe([[1, "Aditya"], [2, "Raj"]], schema)
df4.show()

# Create a DataFrame from a range
df5 = session.range(1, 10).to_df("a")
df5.show()

session.close()
