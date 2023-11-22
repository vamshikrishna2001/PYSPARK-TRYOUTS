from pyspark.sql import *
from pyspark import SparkConf

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.app.name","targaryen")
    conf.set("master","local[*]")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()  
    
    print(spark.conf.get("spark.app.name"))
    