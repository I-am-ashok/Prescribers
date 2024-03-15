from pyspark.sql.functions import explode, length, upper, udf, lit
from pyspark.sql.types import ArrayType, StringType


def convertolist(field):
    ar = []
    for v in field.split(" "):
        ar.append(v)
    return ar


convertolist_udf = udf(convertolist, ArrayType(StringType()))


def Pre_processing(dimension, fact, Preprocessinglogger):
    Preprocessinglogger.info("Preprocessing script has started executing!!!")
    df = dimension.select(upper("city").alias("city"), "state_name", "county_name", "population", "zips") \
        .withColumn("TotalZips_per_city", length("zips")) \
        .withColumn("zip", lit(convertolist_udf("zips"))).drop("zips") \
        .withColumn("Zips", explode("zip"))
    Preprocessinglogger.info(
        "Preprocessing script has been completed successfully and the DataFrame count is " + str(df.count()))
    fact = fact.limit(2).toPandas()
    Preprocessinglogger.info("Fact table schema is " + fact.to_string())
    return df
