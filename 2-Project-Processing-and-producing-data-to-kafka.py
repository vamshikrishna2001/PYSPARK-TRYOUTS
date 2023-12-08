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
        .config("spark.jars.packages" , "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()
        
    print(spark.catalog.listDatabases())
    print(spark.catalog.listTables("taxi_driver"))    
    
    table_df = spark.read.table("taxi_driver.MARTION_SCORESCESE")
    print(table_df.show(5))
    print(table_df.printSchema())
    
    
    df_key_value = table_df.select(f.col('id').alias('key'),f.to_json(f.struct("*")).alias('value'))
    print(df_key_value.show(5))
    
    kafka_conf = ConfigReader.read_spark_config("D:/PYTHON_SPARK/PYSPARK-TRYOUTS/kafka.conf","LOCAL")
    api_secret = ""
    api_key = ""
    # df_key_value.write \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", kafka_conf["bootstrap.servers"]) \
    #     .option("topic", kafka_conf["kafka.topic"]) \
    #     .option("kafka.security.protocol", kafka_conf["security.protocol"]) \
    #     .option("kafka.sasl.jaas.config", kafka_conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
    #     .option("kafka.sasl.mechanism", "PLAIN") \
    #     .option("kafka.client.dns.lookup", kafka_conf["kafka.client.dns.lookup"]) \
    #     .save()
    # df_key_value.write \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092") \
    #     .option("kafka.security.protocol", "SASL_SSL") \
    #     .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(api_key, api_secret)) \
    #     .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    #     .option("kafka.sasl.mechanism", "PLAIN") \
    #     .option("topic","Targaryen") \
    #     .save()
    df_key_value.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
        .option("topic", "Targaryen") \
        .save()

  
    