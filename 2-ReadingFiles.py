from pyspark.sql import *
from pyspark import SparkConf

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.app.name","targaryen")
    conf.set("spark.master","local[*]")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()  
    
    df = spark.read\
        .format("csv") \
        .option("header","true") \
        .option("inferschema","true") \
        .load("sample.csv")
     
    df1 = df.withColumn("Age89",df["Age"]+89)   
    
    
    df_modified = df1.select("Age","Age89","State","Country").groupBy("Country","state").avg()
    # print(df_modified.collect())
    # print(df_modified.show())
    
    # for creating a data base
    df.createOrReplaceGlobalTempView('myTable')
    #    select State,Country,avg(Age) from global_temp.Table groupBy Country ,state 
    df_sparked = spark.sql(
    """
        select Country,state,avg(Age) from global_temp.myTable group by Country,state; 
    """
    )
    print(df_modified.show())
    print(df_sparked.show())
    # spark.stop()