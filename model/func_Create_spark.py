from pyspark.sql import SparkSession

def create_sparkSession():
        spark = SparkSession\
        .builder\
        .appName("Sparkify")\
        .getOrCreate()
        return spark