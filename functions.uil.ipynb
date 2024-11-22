from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import current_timestamp
from pyspark.sql.dataframe import *


def read_files(storage_account, layer, path, file_format):
 
    """
    This function reads all parquet files associated with one folder
    Parameter storage_account is the storage account name
    Parameter layer is the medallion layer: 0-landingzone, 1-bronze, 2-silver, 3-gold
    Parameter path is the relative folder structure that all parquet files should be read from
    Parameter file_format allows for selection of file formats like CSV or Parquet
    """
 
    #constructing full path
    full_path = f"abfss://{layer}@{storage_account}.dfs.core.windows.net/{path}/**"
    message = 'reading file from: ' + full_path
    print(message)
 
    try:
        #reading parquet files to DataFrame
        df = spark.read.option("header",True).format(file_format).load(full_path)
        message = "file successfully read from:" + full_path
        print(message)
            
    except:
        #failure message
        message = "Failed to read file from:" + full_path
        print(message)
 
    return  df



def df_add_columns(df, add_timestamp=False, add_filename=False):
 
    """
    This function adds bronze layer custom fields
    Parameter add_timestamp determines whether timestamp should be added to dataframe
    Parameter add_filename determines whether landing zone path and filename should be added to dataframe
    """
    #add timestamp column
    if add_timestamp:
        df = df.withColumn(
            "xyz_bronze_timestamp",
            current_timestamp()
        )
 
    #add full path and filename column
    if add_filename:
        df = df.withColumn(
            "xyz_bronze_filename",
            input_file_name()
        )
 
    return df
 
#extending DataFrame class with add_columns function 
DataFrame.add_columns = df_add_columns


def write_parquet_table(df, storage_account, layer, table_name):
 
    """
    This function will write dataframe df to storage account as a parquet files
    Parameter table_name defines parquet file name in bronze layer 
    """
 
    #defining full path to in storage account
    full_path = f"abfss://{layer}@{storage_account}.dfs.core.windows.net/{table_name}"    
    message = 'writing bronze file: ' + full_path
    print(message)
    
    try:
        #writing table folder with parquet files in overwrite mode for SCD1
        df.write.mode('overwrite').parquet(path)
        message = 'Table successfully written: ' + full_path
        print(message)
 
    except:
        #error message
        message = 'Failed to write table: ' + full_path
        print(message)