import os

os.environ["environment"] = "DEV"
os.environ["AppName"] = "PysparkApplication"
os.environ["Dimension_Path"] = "..\\staging\\Dimension\\"
os.environ["Fact_Path"] = "..\\staging\\Fact\\"
os.environ["configfile"] = "..\\util\\"

dimensionfile = os.environ["Dimension_Path"] + "us_cities_dimension.parquet"
Factfile = os.environ["Fact_Path"] + "USA_Presc_Medicare_Data_2021.csv"
v_appname = os.environ["AppName"]
environment = os.environ["environment"]
configfile = os.environ["configfile"] + "config.conf"


def environment_value():
    global master
    if environment == "DEV":
        master = "local"
    else:
        master = "yarn"
    return master


v_master = environment_value()

conffile = '..\\util\\config.conf'
main_logger = 'root'
Ingesion_logger = 'Data_ingestion'
sparkobject_logger = 'SparkObject'
Preprocessing_logger = 'Preprocessing'