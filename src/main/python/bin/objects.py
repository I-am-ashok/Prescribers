from src.main.python.bin.SparkObject import sessionobject
from src.main.python.bin.Data_Ingestion import dimensiondf
from src.main.python.bin.Preprocessing import Pre_processing
from Variables import *
import logging
import logging.config


def loggerfunc(conffile, logfile):
    logging.config.fileConfig(conffile)
    logger = logging.getLogger(logfile)
    return logger


from datetime import datetime as dt

date = dt.now().strftime("%F_%X")

dimensionfile = dimensionfile
Factfile = Factfile
mainlogger = loggerfunc(conffile, main_logger)
Ingesionlogger = loggerfunc(conffile, Ingesion_logger)
sparkobjectlogger = loggerfunc(conffile, sparkobject_logger)
Preprocessinglogger = loggerfunc(conffile, Preprocessing_logger)

spark = sessionobject(v_master, v_appname, sparkobjectlogger)
dimension_df, fact_df = dimensiondf(spark, dimensionfile, Factfile, Ingesionlogger)
Pre_processing_df=Pre_processing(dimension_df, fact_df,Preprocessinglogger)