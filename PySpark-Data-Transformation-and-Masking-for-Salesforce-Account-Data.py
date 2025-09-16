from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType,
    FloatType, DoubleType, DecimalType, DateType,
    TimestampType, NumericType  
)


# 1. Load source table (simulate)


# If you're using SQL data:
df = spark.sql("SELECT * FROM default.account")


# 2. Define transformation logic


# Type definitions
string_types = (StringType,)
numeric_types = (IntegerType, LongType, ShortType, FloatType, DoubleType, DecimalType)
date_types = (DateType,)
timestamp_types = (TimestampType,)

# Hardcoded schema if needed (for testing)
schema = dict(df.dtypes)

# Column-specific override rules
column_rules = {
    # No special rules here for now
}

# Sensitive column masking
sensitive_cols = ["Type"]
MASK_STRING = "****"

# General transformation logic
def transform_column(col_name, dtype):
    if col_name in column_rules:
        return column_rules[col_name](col_name)
    elif col_name in sensitive_cols:
        return F.lit(MASK_STRING).alias(col_name)
    elif isinstance(dtype, NumericType):
        return (F.col(col_name) + F.lit(1000)).alias(col_name)
    elif isinstance(dtype, StringType):
        # Updated: second part is literal column name
        return F.concat(F.upper(F.col(col_name)), F.lit("_"), F.lit(col_name)).alias(col_name)
    elif isinstance(dtype, (DateType, TimestampType)):
        return F.date_format(F.col(col_name), 'yyyy-MM-dd').alias(col_name)
    else:
        return F.col(col_name)


#  Apply transformations

transformed_cols = [
    transform_column(col_name, df.schema[col_name].dataType)
    for col_name in df.columns
]

df_transformed = df.select(*transformed_cols)


#  Output 

df_transformed.createOrReplaceTempView("transformed_account")


# df_transformed.show(truncate=False)
