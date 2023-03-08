"""
connector.py holds the following helper functions specific to pandas DataFrames:

- get_col_names(table)                                    : Gets all column names
- get_unique_values(table, col)                           : Gets all unique values in a given column
- add_col(table, col, default)                            : Creates a column where all values equal the default value
- get_table_length(table)                                 : Gets the length of a DataFrame
- rename_col(table, col, to)                              : Renames a given column

"""

import pyspark
from labelbase.connector import validate_column_name_change
from pyspark.sql.functions import udf, lit
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from labelbox import Client as labelboxClient
from labelbase.metadata import get_metadata_schema_to_name_key
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
import json

def check_pyspark():
    try:
        import pyspark.pandas as pd
    except:
        raise RuntimeError(f'labelspark.Client() requires pyspark to be installed - please update your Databricks runtime to support pyspark')
    
def get_col_names(table:pyspark.sql.dataframe.DataFrame, extra_client=None):
    """Grabs all column names from a Spark Dataframe
    Args:
        table           :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
        extra_client    :   Ignore this value - necessary for other labelbase integrations
    Returns:
        List of strings corresponding to all column names
    """
    return [str(col[0]) for col in table.dtypes]    


def get_unique_values(table:pyspark.sql.dataframe.DataFrame, col:str, extra_client=None):
    """ Grabs all unique values from a Spark Dataframe column as strings
    Args:
        table           :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
        col             :   Required (str) - Spark Table column name
        extra_client    :   Ignore this value - necessary for other labelbase integrations
    Returns:
        List of unique values from a spark table column as strings
    """
    return [str(x.__getitem__(column_name)) for x in table.select(column_name).distinct().collect()]

def add_col(table:pyspark.sql.dataframe.DataFrame, col:str, default_value="", extra_client=None):
    """ Adds a column of empty values to an existing Spark Dataframe
    Args:
        table           :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
        col             :   Required (str) - Spark Table column name
        default         :   Optional (any) - Value to insert for every row in the newly created column
        extra_client    :   Ignore this value - necessary for other labelbase integrations
    Returns:
        Your table with a new column given the column_name and default_value  
    """
    table = table.withColumn(column_name, pyspark.sql.functions.lit(default_value))
    return table

def get_table_length(table:pyspark.sql.dataframe.DataFrame, extra_client=None):
    """ Tells you the size of a Spark Dataframe
    Args:
        table           :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
        extra_client    :   Ignore this value - necessary for other labelbase integrations
    Returns:
        The length of your table as an integer
    """  
    return len(table)

def rename_col(table: pandas.core.frame.DataFrame, col:str, to:str):
    """ Renames columns into the Labelspark format    
    Args:
        table           :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
        col             :   Required (str) - Existing column name to-be-changed
        to              :   Required (str) - What to rename the column as       
    Returns:
        Updated `table` object with renamed column
    """
    existing_cols = get_col_names(table, extra_client=False)
    validate_column_name_change(old_col_name=col, new_col_name=to, existing_col_names=existing_cols)
    table = table.rename(columns={col:to})
    return table                  
  
def upsert_function(upsert_dict_bytes, global_key_col, metadata_value_col):
    """ Nested UDF Functionality to upsert a column in a Databricks Spark table given a dictionary where 
    Args:  
        upsert_dict_bytes               :   Required (bytes) - Bytearray representation of a dictionary where {key=global_key : value=new_value}
        global_key_col                  :   Required (str) - Column name for the global_key
        metadata_value_col              :   Required (str) - Target column name
    Returns:
        New value to-be-inserted in the column corresponding to this metadata field
    """  
    upsert_dict = json.loads(upsert_dict_bytes)
    if global_key_col in upsert_dict.keys():
        return_value = upsert_dict[global_key_col]
    else:
        return_value = metadata_value_col
    return return_value

