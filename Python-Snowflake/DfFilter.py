from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from ConnectionProperties import ConnectionProperties

conn = ConnectionProperties()
connection_parameters =  conn.getconnection()

# create the connection 
session = Session.builder.configs(connection_parameters).create()

# filter the table
df1 = session.table("sample_product_data").filter(col("id") == 10)
df1.show()

# select the columns
df2 = session.table("sample_product_data").select(col("ID"), col("Name"), col("Serial_Number"))
df2.show()

# Import the col function from the functions module.
df = session.table("sample_product_data")
df3 = df.select(df["id"], df["serial_number"])
df3.show()

df4 = df.select(df.id, df.name)
df4.show()

df5 = df.select("serial_number", "name")
df5.show()

session.close()