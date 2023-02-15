from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from ConnectionProperties import ConnectionProperties
from copy import copy

conn = ConnectionProperties()

connection_parameters = conn.getconnection()

# create the connection 
session = Session.builder.configs(connection_parameters).create()

# create 2 dataframes
df1 = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "val1"])
df2 = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "val2"])

# join on key column 
res_df = df1.join(df2, df1.col("key") == df2.col("key")).select(df1["key"].as_("key") , "val1", "val2")
res_df.show()

# Both dataframes have the same column "key", the following is more convenient.
df1.join(df2, ["key"]).show()

# Use & operator connect join expression. '|' and ~ are similar.
df1.join(df2, (df1["key"] == df2["key"]) & (df1["val1"] < df2["val2"])).select(df1["key"].as_("key"), "val1", "val2").show()

df1.join(df2, (df1["key"] == df2["key"]) & (df1["val1"] > df2["val2"])).select(df1["key"].as_("key"), "val1", "val2").show()

# self join
# below code should throw exception " You cannot join a DataFrame with itself because the column references cannot be resolved correctly. " 
# ambiguous code 
# df1.join(df1, ["key"]).show()

# so we should use copy function for self join
df3 = copy(df1)
df1.join(df3, ["key"]).show()

# Note that when there are overlapping columns in the Dataframes, 
# Snowpark will prepend a randomly generated prefix to the columns in the join result:
# so below code will throw exception as 
# 'Column' object has no attribute '_as'
# df1.join(df3, ["key"]).select(df1["key"]._as("key"), df1["val1"]._as("val1"), df3["val1"]._as("val2")).show()


# we can reference the overlapping columns using Column.alias:
df1.join(df3, ["key"]).select(df1["key"].alias("key1"), df3["key"].alias("key2"), df1["val1"].alias("val1"), df3["val1"].alias("val2")).show()

# To avoid random prefixes, you could specify a suffix to append to the overlapping columns:
df1.join(df3, ["key"], lsuffix= "_left", rsuffix="_right").show()

session.close()