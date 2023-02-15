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

# In some cases, the column name might contain double quote characters:
session.sql('''
    create or replace temp table quoted(
    "name_with_""air""_quotes" varchar,
    """column_name_quoted""" varchar
    )''').collect()

session.sql('''insert into quoted ("name_with_""air""_quotes", """column_name_quoted""") values ('a', 'b')''').collect()

df = session.table("quoted")
df.show()
"""
for each double quote character within a double-quoted identifier,
you must use two double quote characters 
(e.g. "name_with_""air""_quotes" and 'column_name_quoted')
"""
df1 = df.select(col("\"name_with_\"\"air\"\"_quotes\""))
df1.show()

df2 = df.select("\"\"\"column_name_quoted\"\"\"")
df2.show()

session.close()