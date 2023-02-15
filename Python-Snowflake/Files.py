from snowflake.snowpark.types import * 
from snowflake.snowpark import Session
from ConnectionProperties import ConnectionProperties
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException
# Import the sql_expr function from the functions module.
from snowflake.snowpark.functions import sql_expr


conn = ConnectionProperties()
connection_parameters =  conn.getconnection()

# create the connection 
session = Session.builder.configs(connection_parameters).create()
df_schema = StructType([StructField("id", StringType),
StructField("name", StringType)
])

# Call the schema property in the DataFrameReader object
"""
The schema property returns a DataFrameReader object that is configured to read files containing the specified fields.

Note that you do not need to do this for files in other formats (such as JSON).
For those files, the DataFrameReader treats the data as a single field of the
VARIANT type with the field name $1.
"""
reader = session.read.schema(df_schema)

"""
The following example sets up the DataFrameReader object to query data in a CSV file 
that is not compressed and that uses a semicolon for the field delimiter.
The option and options methods return a DataFrameReader object that is configured with the specified options.
"""
df_reader = df_reader.option("field_delimiter", ";").option("COMPRESSION", "NONE")

#Call the method corresponding to the format of the file (e.g. the csv method),
#  passing in the location of the file.

df = df_reader.csv("@s3_ts_stage/emails/data_0_0_0.csv")

"""
Use the DataFrame object methods to perform any transformations needed on the dataset
(for example, selecting specific fields, filtering rows, etc.).
For example, to extract the color element from a JSON file in the stage named my_stage:
"""
df = session.read.json("@my_stage").select(sql_expr("$1:color"))


"""
 sql_expr function does not interpret or modify the input argument. 
 The function just allows you to construct expressions and snippets in SQL 
 that are not yet supported by the Snowpark API.
"""