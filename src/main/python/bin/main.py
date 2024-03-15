from objects import *

if __name__ == '__main__':
    mainlogger.info(f"Dimension file count is {dimension_df.count()}")
    mainlogger.info(f"Fact file count is {fact_df.count()}")
    mainlogger.info("the 2 dataframes have been loaded successfully!!!")
    mainlogger.info("Preprocessing script is calling!!")
    Pre_processing_df = Pre_processing_df.limit(2).toPandas()
    mainlogger.info("The Preprocessing dataframe is \n"+Pre_processing_df.to_string())
