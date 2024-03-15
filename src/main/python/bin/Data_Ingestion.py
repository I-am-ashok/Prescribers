def dimensiondf(spark,dimensionfile,Factfile,Ingesionlogger):
    Ingesionlogger.info("spark data loading is in progress .......")
    dimension = spark.read.parquet(dimensionfile)
    fact = spark.read.csv(Factfile, header=True, inferSchema=True)
    Ingesionlogger.info("spark dataframes have been created ..")
    return dimension, fact



