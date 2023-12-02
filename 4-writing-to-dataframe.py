from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("targaryen") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read \
        .format("parquet") \
        .option("header", "true") \
        .option("inferschema", "true") \
        .load("flight-time.parquet")

    df = df.repartition(4) ### based up the number of partitions those many files will be created 
    df.write.format('csv').mode('overwrite').option('path',"D://PYTHON_SPARK//PYSPARK-TRYOUTS//DataSink//").save()
    print(df.groupBy(spark_partition_id()).count().show())
    
    
    ### ****** partitioning based up the columns 
    df = df.coalesce(1) ## if we don't partition if we will get 4 files in each folder 
    df.write.format('csv').mode('overwrite').option('path',"D://PYTHON_SPARK//PYSPARK-TRYOUTS//DataSink_column_partition//").partitionBy("OP_CARRIER","ORIGIN").save()
    print(df.groupBy(spark_partition_id()).count().show())

    
    