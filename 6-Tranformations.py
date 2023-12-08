from pyspark.sql import *
from pyspark import SparkConf
from pyspark.sql.functions import *

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.app.name","targaryen")
    conf.set("spark.master","local[*]")
    
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()  
    
    print(spark.conf.get("spark.app.name"))
    print(spark.sparkContext.getConf().toDebugString())    
    
    df = spark.read.text('apache_logs.txt')
    pattern = r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<timestamp>[^\]]+)\] "(?P<request>[^"]+)" (?P<status>\d+) (?P<size>\d+) "(?P<referrer>[^"]+)" "(?P<user_agent>[^"]+)"'
    df = df.withColumnRenamed("value","log_entry")
    df_result = df.withColumn("ip", regexp_extract("log_entry", pattern, 1)) \
              .withColumn("timestamp", regexp_extract("log_entry", pattern, 2)) \
              .withColumn("request", regexp_extract("log_entry", pattern, 3)) \
              .withColumn("status", regexp_extract("log_entry", pattern, 4)) \
              .withColumn("size", regexp_extract("log_entry", pattern, 5)) \
              .withColumn("referrer", regexp_extract("log_entry", pattern, 6)) \
              .withColumn("user_agent", regexp_extract("log_entry", pattern, 7))
              
    print(df_result.show(5))