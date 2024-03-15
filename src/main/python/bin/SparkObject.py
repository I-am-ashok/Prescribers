from pyspark.sql import SparkSession



def sessionobject(v_master, v_appname,sparkobjectlogger):
    sparkobjectlogger.info("SPark Session is creating...")
    spark = SparkSession.builder.master(v_master).appName(v_appname).getOrCreate()
    sparkobjectlogger.info("spark session has been created ...")
    return spark
