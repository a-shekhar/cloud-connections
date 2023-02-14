class ConnectionProperties:
    # to create connection without existing DB 
    def getconnectionwithoutDB(self):
        connection_parameters = {
        "account" : "<your Snowflake account i.e account identifier>",
        "user": "<your snowflake user id>",
        "password": "<your snowflake password>",
        "role": "<your snowflake role>",  # optional
        "warehouse": "<your snowflake warehouse>",  # optional
        # "database": "<your snowflake database>",  # optional
        # "schema": "<your snowflake schema>",  # optional
        }  
        return connection_parameters

    # to create connection
    def getconnection(self):
        connection_parameters = {
        "account" : "<your Snowflake account i.e account identifier>",
        "user": "<your snowflake user id>",
        "password": "<your snowflake password>",
        "role": "<your snowflake role>",  # optional
        "warehouse": "<your snowflake warehouse>",  # optional
        "database": "<your snowflake database>",  # optional
        "schema": "<your snowflake schema>",  # optional
        }  
        return connection_parameters
