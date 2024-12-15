import os
import urllib.request

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)



# Set Hadoop environment variables
os.environ['HADOOP_HOME'] = r'C:\Users\vaish\hadoop\hadoop-3.0.0'
os.environ['PATH'] += os.pathsep + r'C:\Users\vaish\hadoop\hadoop-3.0.0\bin'
os.environ['JAVA_HOME'] = r'C:\Users\vaish\.jdks\corretto-1.8.0_432'


urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
}

for url, path in urls_and_paths.items():
    urllib.request.urlretrieve(url, path)


# ======================================================================================

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
import os
from collections import namedtuple
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\vaish\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

# spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder \
    .appName("Large Dataset Anonymization") \
    .getOrCreate()

# spark.read.format("csv").load("data/test.txt").toDF("Success").show(20,False)

###############################################################################################
import random
import string



# Read the CSV file
# df = spark.read.csv("C:\\Users\\vaish\\Test_data\\MOCK_DATA.csv", header=True, inferSchema=True)
try:
    # df = spark.read.csv(path, header=True, inferSchema=True)
    df = spark.read.csv("C:\\Users\\vaish\\Test_data\\MOCK_DATA.csv", header=True, inferSchema=True)
    df.show()
except Exception as e:
    print(f"Error: {e}")
# Select required columns
required_columns = ['first_name', 'last_name', 'address', 'date_of_birth']
df = df.select(*required_columns)

# Drop rows with null values
df = df.dropna()

# Function to generate random string data
def generate_random_string(length=8):
    return ''.join(random.choices(string.ascii_letters, k=length))

# Define UDF (User-Defined Function) for random string generation
random_string_udf = udf(lambda: generate_random_string(), StringType())

# Apply UDF to columns that need anonymization
df = df.withColumn('first_name', random_string_udf()) \
    .withColumn('last_name', random_string_udf()) \
    .withColumn('address', random_string_udf())

# Save the anonymized data to a new CSV file
df.write.csv("C:\\Users\\vaish\\Test_data\\MOCK_DATA.csv", header=True, mode="overwrite")

# Show a sample of the anonymized data
df.show(truncate=False)


