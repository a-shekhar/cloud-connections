"""
Snowflake Documentaton:

Unquoted object identifiers:

Start with a letter (A-Z, a-z) or an underscore (“_”).

Contain only letters, underscores, decimal digits (0-9), and dollar signs (“$”).

Are stored and resolved as uppercase characters (e.g. id is stored and resolved as ID).

If you put double quotes around an identifier (e.g. “My identifier with blanks and punctuation.”), the following rules apply:

The case of the identifier is preserved when storing and resolving the identifier (e.g. "id" is stored and resolved as id).

The identifier can contain and start with ASCII, extended ASCII, and non-ASCII characters.

"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from ConnectionProperties import ConnectionProperties
from copy import copy

conn = ConnectionProperties()

connection_parameters = conn.getconnection()

# create the connection 
session = Session.builder.configs(connection_parameters).create()
#  name is case insensitive because it's not quoted.
# age and 'ID with Space' are case sensetive since it is double quoted 
df = session.sql("""CREATE OR REPLACE TABLE "Aditya10"
(name varchar, 
"Age" int, 
"Id with Space" varchar 
)
""").collect()

session.sql("""
INSERT INTO "Aditya10" (name, "Age", "Id with Space") VALUES ('Aditya', 27, 'ABC')
""").collect()

# you must use double quotes (") around the name. Use a backslash (\) to escape the double quote character within a string litera
df1 = session.table("\"Aditya10\"")
df1.show()
# Alternatively, you can use single quotes instead of backslashes to escape the double quote character within a string literal.
df2 = session.table('"Aditya10"').show()
# here both query should work since name is case insensetive
res1 = df1.select(col("Name"))
res1.show()

res2 = df1.select(col("NAME"))
res2.show()

# these 2 statements will work
res3 = df1.select(col('"Age"')).show()
res4 = df1.select(col('"Id with Space"')).show()

# these statements won't work
#res3 = df1.select(col("Age")).show()
#res4 = df1.select(col("Id with Space")).show()
#res5 = df1.select(col('"AGE"')).show()
#res6 = df1.select(col('"ID with Space"')).show()

session.close()