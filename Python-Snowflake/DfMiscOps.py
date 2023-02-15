from snowflake.snowpark import Session
from ConnectionProperties import ConnectionProperties
# Import for the lit and col functions.
from snowflake.snowpark.functions import col, lit
# Import for the DecimalType class.
from snowflake.snowpark.types import DecimalType


conn = ConnectionProperties()
connection_parameters =  conn.getconnection()

# create the connection 
session = Session.builder.configs(connection_parameters).create()

df = session.table("sample_product_data")
df.show()

""" 
a literal in a method that takes a Column object 
as an argument, create a Column object for the literal 
by passing the literal to the lit function
"""


# Show the first 10 rows in which category_id is greater than 5.
# Use `lit(5)` to create a Column object for the literal 5.
df_filtered = df.filter(col("CATEGORY_ID") > lit(5))
df_filtered.show()

# To cast a Column object to a specific type, call the cast method
# to cast a literal as a NUMBER with a precision of 5 and a scale of 2
cas = lit(0.05).cast(DecimalType(5,2))
print(cas)

"""
To retrieve the definition of the columns in the dataset for the DataFrame, call the schema property. 
This method returns a StructType object that contains an list of StructField objects. 
Each StructField object contains the definition of a column.
"""
# case insensitive columns name will be returned in UPPER CASE and case sensitive columns will be returned as it was tYPed
schema = df.schema
print(schema)


# we can also filter column names 
selected_col = df.select("id", "3rd")
print(selected_col.schema.names)


"""
 the DataFrame is lazily evaluated, which means the SQL statement isn’t sent to the server for execution 
 until you perform an action. An action causes the DataFrame to be evaluated 
 and sends the corresponding SQL statement to the server for execution.

The following methods perform an action:
collect - Evaluates the DataFrame and returns the resulting dataset as an list of Row objects.
count - Evaluates the DataFrame and returns the number of rows.
show - Evaluates the DataFrame and prints the rows to the console. Note that this method limits the number of rows to 10 (by default).
save_as_table - Saves the data in the DataFrame to the specified table. 
"""

# this does not execute any query 
df = session.table("sample_product_data").select(col("id"), col("name"))
# but this does and returns the result
print(df.collect())

# print the count of rows in the table.
print("Row count is", df.count())

# Limit the number of rows to 5, rather than 10.
df.show(5)

""" If you are calling the schema property to get the definitions of the columns in the DataFrame, 
you do not need to call an action method.
"""

# To return the contents of a DataFrame as a Pandas DataFrame, use the to_pandas method.
python_df = session.create_dataframe(["a", "b", "c"])
panda_df = python_df.to_pandas()
print(panda_df)


session.close()