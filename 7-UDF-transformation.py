import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UDF Demo") \
        .master("local[2]") \
        .getOrCreate()

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("survey.csv")

    survey_df.show(10)

    parse_gender_udf = udf(parse_gender, returnType=StringType())
    survey_df2 = survey_df.withColumn("Gender1", parse_gender_udf("Gender"))
    print(survey_df2.columns)
    print(survey_df2.show(5))

    # spark.udf.register("parse_gender_udf", parse_gender, StringType())
    # print("Catalog Entry:")
    # [print(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]

    # survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    # survey_df3.show(10)