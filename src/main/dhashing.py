import hashlib
import os
import pathlib

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType

CURRENT_PATH = pathlib.Path(__file__).parent.absolute()
INPUT_FILE = os.path.abspath(os.path.join(CURRENT_PATH, '../resources/people.csv'))
HASHING_OUTPUT_PATH = os.path.abspath(os.path.join(CURRENT_PATH, '../resources/hashing_output'))
ACTIVATION_OUTPUT_PATH = os.path.abspath(os.path.join(CURRENT_PATH, '../resources/table_activation'))
SALT = str("#8b%$K!z")


def hashing_value(hashing_key):
    sha_value = hashlib.sha512((hashing_key + SALT).encode()).hexdigest()

    return sha_value


hashing_udf = udf(hashing_value, StringType())

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master('local') \
        .appName('column_encryption') \
        .getOrCreate()

    df = spark.read.option("encoding", "UTF-8") \
        .csv(INPUT_FILE
             , sep=";"
             , inferSchema=True
             , header=True).cache()

    # Hashing Name
    hashing = df.withColumn('hashed_name', hashing_udf('name'))
    hashing.show(truncate=False)

    # Saving Name Values to Activate
    activation_name = hashing.withColumn("table", lit("user")).withColumn("key_type", lit("name"))
    # activation_name.show(truncate=False)

    activation_name\
        .drop("email")\
        .drop("age")\
        .drop("job")\
        .write.mode('append')\
        .partitionBy("table", "key_type")\
        .parquet(ACTIVATION_OUTPUT_PATH)

    # Hashing Email
    hashing = hashing.withColumn('hashed_email', hashing_udf('email'))
    hashing.show(truncate=False)

    # Saving Email Values to Activate
    activation_email = hashing.withColumn("table", lit("user")).withColumn("key_type", lit("email"))
    # activation_email.show(truncate=False)

    activation_email \
        .drop("name") \
        .drop("age") \
        .drop("job") \
        .drop("hashed_name") \
        .write.mode('append') \
        .partitionBy("table", "key_type") \
        .parquet(ACTIVATION_OUTPUT_PATH)

    # Saving new Table with Hashing values
    hashing \
        .drop("email") \
        .drop("name") \
        .write.mode('overwrite') \
        .parquet(HASHING_OUTPUT_PATH)

    print("End !")
