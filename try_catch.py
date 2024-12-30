from snowflake.snowpark.exceptions import SnowparkJoinException

df = session.table("sample_product_data")
# This fails because columns named "id" and "parent_id"
# are in the left and right DataFrames in the join.
try:
  df_joined = df.join(df, col("id") == col("parent_id")) # fails
except SnowparkJoinException as e:
  print(e.message)
