from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, lit, explode, sum as _sum
from typing import List


def main():
    print("Word count example with Pyspark")
    spark_session = SparkSession.builder \
                                .master("local") \
                                .appName("Pyspark Hello World") \
                                .getOrCreate()
    
    file_path = input("Enter file path: ")
    text_input = spark_session.read.text(file_path)
    print(text_input.select(explode(split(col("value"), "\s")).alias("words")) \
                    .groupBy(col("words")) \
                    .count() \
                    .show())

if __name__ == "__main__":
    main()
