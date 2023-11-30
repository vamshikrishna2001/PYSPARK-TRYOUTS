############ PARTITION ##############

# so partitioning ... df.partition does the partition its 
# basically used when we want to divide the dat into many parts 
# so that each part can be fed to the executor core

# ***** COALESCE ********
# it is used in the scenarios of scale down for min data movement

# ********* JOB ************
# Each ultimate action will create a job 

# *********** STAGE *********
# Each shuffle or repartition operation creates a state in the job

# *********** Task **********
# Basically number of partitions will be equal to the number of tasks in a stage 


# NOTE 
# Each task is taken up by each core 


from pyspark.sql import *
from pyspark import SparkConf

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.app.name","targaryen")
    conf.set("master","local[*]")
    conf.set("spark.sql.shuffle.partitions",2) # the grouby operation in line 47 wil result in 2 partitions with this config
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()  
    
    #** THIS READ OPERATION CONSISTS OF TWO JOBS WILL BE CREATED CAN CHECK IN SPARK UI ONE FOR READING AND OTHER FOR INFERING SCHEMA

    df = spark.read\
        .format("csv") \
        .option("header","true") \
        .option("inferschema","true") \
        .load("sample.csv")
     
    # THE BELOW OPERATION consists of only one action so one job  
    
    df.repartition(2) ## this creates a stage and goes to WORK EXCHANGE
    
    df = df.select("Age","Country","State").groupBy("Country").count() ## since groupby is a wide transformation it internally shuffles the stuff and sends to WRITE EXCHANGE 
    # we have another stage reading the WRITE EXCHAGE and does the count operation
    
    print(df.collect()) # ---> this is the ultimate action
    input() # check the localhost:4040
