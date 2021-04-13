from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import udf
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

import pathlib

CURRENT_PATH = pathlib.Path(__file__).parent.absolute()


def encrypt_value(mobno):
    sha_value = hashlib.sha256(mobno.encode()).hexdigest()
    # sha_value2 = hashlib.sha512()

    return sha_value


spark_udf = udf(encrypt_value, StringType())

if __name__ == '__main__':
    print(" Hii")

    spark = SparkSession.builder \
        .master('local') \
        .appName('column_encryption') \
        .getOrCreate()

    print(CURRENT_PATH)
    # /Users/pquintero/github-workspace/dhasing/src/main

    df = spark.read.option("encoding", "UTF-8") \
        .csv("/Users/pquintero/github-workspace/dhasing/src/resources/people.csv"
             , sep=";"
             , inferSchema=True
             , header=True).cache()

    df.printSchema()

    # Encrypted Name
    hashing = df.withColumn('encrypted_name', spark_udf('name'))
    hashing.show(truncate=False)

    # Encrypted Email
    hashing = hashing.withColumn('encrypted_email', spark_udf('email'))
    hashing.show(truncate=False)

    print("Bye !")

    # Import the os module
    import os

