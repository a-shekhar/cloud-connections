from snowflake.snowpark import Session
from ConnectionProperties import ConnectionProperties

conn = ConnectionProperties()
connection_parameters = conn.getconnection()

# create the connection 
session = Session.builder.configs(connection_parameters).create()

# create dataframe from data in table
df_table = session.table("sample_product_data")
# show first 10 rows
df_table.show()

# Where condition
where_cl = session.sql("SELECT category_id, serial_number, name FROM sample_product_data WHERE id = 10")
where_cl.show()

session.close() 