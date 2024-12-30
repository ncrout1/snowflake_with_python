# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from copy import copy

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    # tableName = 'OUR_FIRST_DB.PUBLIC.ORDERS'
    # dataframe = session.table(tableName).filter(col("order_id")==1)
    # B-25601
    # session.sql("select * from OUR_FIRST_DB.PUBLIC.ORDERS").collect()

    # Print a sample of the dataframe to standard output.
    # dataframe.show()
    df_lhs = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "value1"])
    df_rhs = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "value2"])
    df_lhs_copied = copy(df_lhs)
    df_self_joined = df_lhs.join(df_lhs_copied, (df_lhs.col("key") == df_lhs_copied.col("key")) & (df_lhs.col("value1") == df_lhs_copied.col("value1")))

    # Return value will appear in the Results tab.
    return df_self_joined
