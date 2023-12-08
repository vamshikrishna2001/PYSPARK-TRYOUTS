from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as f
from Utils import ConfigReader


if __name__ == "__main__":
    conf = SparkConf()
    spark_config = ConfigReader.read_spark_config("D:/PYTHON_SPARK/PYSPARK-TRYOUTS/spark.conf","LOCAL")
    # print(spark_config['spark.sql.catalogImplementation'])
    spark = SparkSession.builder \
        .config("spark.master", spark_config["spark.master"]) \
        .config("spark.app.name", spark_config["spark.app.name"]) \
        .config("spark.sql.catalogImplementation" , 'hive') \
        .getOrCreate()
        
    df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("taxi_fare.csv")
    spark.sql("CREATE DATABASE IF NOT EXISTS TAXI_DRIVER")
    spark.catalog.setCurrentDatabase("TAXI_DRIVER")
    df = df.repartition(5)
    df.write.format("csv").mode("overwrite").saveAsTable("MARTION_SCORESCESE")

