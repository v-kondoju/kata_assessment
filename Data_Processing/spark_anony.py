from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id

# Spark Session
spark = SparkSession.builder.getOrCreate()

# Function to anonymize column(s)
def anonymize_column(spark_df, column_name):
    return spark_df.withColumn(column_name, sha2(concat_ws("_", col(column_name), monotonically_increasing_id()), 256))

# Load Parquet file from the local computer
input_parquet_file = "input_parquet_file.parquet"
# Path where the output parquet will be saved
output_parquet_path = "*\\output.parquet"

spark_df = spark.read.parquet(input_parquet_file)
# List of columns to anonymize
columns_to_anonymize = ["First_name", "Last_name", "Address"]
for column in columns_to_anonymize: # Iterating through columns to be anonymized from the list
    spark_df = anonymize_column(spark_df, column)

# writing the parquet file tp the target location
spark_df.write.parquet(output_parquet_path, mode="overwrite")
