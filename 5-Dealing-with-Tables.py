from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext , SparkConf

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.app.name","targaryen")
    conf.set("master","local[*]")
    conf.set("spark.sql.catalogImplementation", "hive") ## overwriting current metastore to use hive metastore
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()  
    df = spark.read \
        .format("parquet") \
        .option("header", "true") \
        .option("inferschema", "true") \
        .load("flight-time.parquet")
        
        
    spark.sql("CREATE DATABASE IF NOT EXISTS AIR_LINES")
    spark.catalog.setCurrentDatabase("AIR_LINES")
    df.write.format("csv").mode("overwrite").bucketBy(5,"OP_CARRIER","ORIGIN").sortBy("OP_CARRIER","ORIGIN").saveAsTable("AIR_TABLE")
    # BY DEFAULT THE DATABASE IS Created in parquet files so i want to investiagte it so iam writing it into a csv file 
    # bucket basically does the hashing on the columns specified to make sure we wont end up more than 5 partitions 
    # sort by can also be added to see all "OP_CARRIER" + "ORIGIN" in order
    print(spark.catalog.listDatabases())
    print(spark.catalog.listTables("AIR_LINES"))