"""
connector.py holds the following helper functions specific to pandas DataFrames:

- get_col_names(table)                                    : Gets all column names
- get_unique_values(table, col)                           : Gets all unique values in a given column
- add_col(table, col, default)                            : Creates a column where all values equal the default value
- get_table_length(table)                                 : Gets the length of a DataFrame
- rename_col(table, col, to)                              : Renames a given column

"""

import pyspark

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
    return [str(x.__getitem__(col)) for x in table.select(col).distinct().collect()]

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
    table = table.withColumn(col, pyspark.sql.functions.lit(default_value))
    return table

def get_table_length(table:pyspark.sql.dataframe.DataFrame, extra_client=None):
    """ Tells you the size of a Spark Dataframe
    Args:
        table           :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
        extra_client    :   Ignore this value - necessary for other labelbase integrations
    Returns:
        The length of your table as an integer
    """  
    return table.count()

def rename_col(table:pyspark.sql.dataframe.DataFrame, col:str, to:str):
    """ Renames columns into the Labelspark format    
    Args:
        table           :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
        col             :   Required (str) - Existing column name to-be-changed
        to              :   Required (str) - What to rename the column as       
    Returns:
        Updated `table` object with renamed column
    """
    table = table.withColumnRenamed(col,to)
    return table                  
 
