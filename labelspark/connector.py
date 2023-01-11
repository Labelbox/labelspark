from pyspark.sql.functions import udf, lit
from pyspark.sql.dataframe import DataFrame
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from labelbase import Client as labelbaseClient
from labelbox import Client as labelboxClient
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
import json

def check_pyspark():
    try:
        import pyspark.pandas as pd
    except:
        raise RuntimeError(f'labelspark.Client() requires pyspark to be installed - please update your Databricks runtime to support pyspark')

def create_upload_dict(table:DataFrame, lb_client:labelboxClient, base_client:labelbaseClient, row_data_col:str, 
                       global_key_col:str="", external_id_col:str="", metadata_index:dict={}, local_files:bool=False, 
                       divider:str="///", verbose=False):
    """ Uses UDFs to create a column of data row dictionaries to-be-uploaded, then converts this column into a list
    Args:
        table                       :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
        lb_client                   :   Required (labelbox.client.Client) - Labelbox Client object
        base_client                 :   Required (labelbase.client.Client) - Labelbase Client object
        row_data_col                :   Required (str) - Column containing asset URL or file path
        global_key_col              :   Optional (str) - Column name containing the data row global key - defaults to row data
        external_id_col             :   Optional (str) - Column name containing the data row external ID - defaults to global key
        metadata_index              :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type}
                                            metadata_type must be either "enum", "string", "datetime" or "number"
        divider                     :   Optional (str) - String delimiter for all name keys generated for parent/child schemas
        verbose                     :   Optional (bool) - If True, prints details about code execution; if False, prints minimal information
    Returns:
        Two values:
        - global_key_to_upload_dict - Dictionary where {key=global_key : value=data row dictionary in upload format}
        - errors - List of dictionaries containing conversion error information; see connector.create_data_rows() for more information
    """        
    global_key_col = global_key_col if global_key_col else row_data_col
    external_id_col = external_id_col if external_id_col else global_key_col  
    metadata_schema_to_name_key = base_client.get_metadata_schema_to_name_key(lb_mdo=False, divider=divider) 
    uploads_table = create_uploads_column(
        table=table, lb_client=lb_client, row_data_col=row_data_col, global_key_col=global_key_col, external_id_col=external_id_col, 
        metadata_schema_to_name_key=metadata_schema_to_name_key, metadata_index=metadata_index
    )
    upload_list = uploads_table.select("uploads").rdd.map(lambda x: x.uploads.asDict()).collect()
    global_key_to_upload_dict = {data_row_dict['global_key'] : data_row_dict for data_row_dict in upload_list}
    return global_key_to_upload_dict

def create_uploads_column(table:DataFrame, lb_client:labelboxClient, row_data_col:str, 
                          global_key_col:str, external_id_col:str, metadata_name_key_to_schema:dict, 
                          metadata_schema_to_name_key:dict, metadata_index:dict={}, divider:str="///"):
    """ Creates a spark table with an "uploads" that can be queried and uploaded to Labebox
    Args:
        table                       :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
        lb_client                   :   Required (labelbox.client.Client) - Labelbox Client object
        row_data_col                :   Required (str) - Column containing asset URL or file path
        global_key_col              :   Required (str) - Column name containing the data row global key - defaults to row data
        external_id_col             :   Required (str) - Column name containing the data row external ID - defaults to global key
        metadata_schema_to_name_key :   Required (dict) - Dictionary where {key=metadata_schema_id : value=metadata_field_name_key}
        metadata_index              :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type}
                                            metadata_type must be either "enum", "string", "datetime" or "number"
        divider                     :   Optional (str) - String delimiter for all name keys generated for parent/child schemas
    Returns:
        Updated version of the input `spark_table` with an `uploads` which is a dictionary of data rows to-be-uploaded to Labelbox
    Uses:
        __create_upload_data_row_values
        attach_metadata_to_data_row_values
    """
    # Create your column's syntax - this is chosen to mimic Labelbox's import format for data row dictionaries
    upload_schema = StructType([
        StructField("row_data", StringType()),
        StructField("global_key", StringType()),
        StructField("external_id", StringType()),
        StructField("metadata_fields", ArrayType(MapType(StringType(), StringType(), True)))
    ])            
    # Run your __create_upload_data_row_values UDF, creating a new table in the process
    create_data_rows_udf = udf(__create_data_rows_udf, upload_schema)
    table = table.withColumn('uploads', create_data_rows_udf(row_data_col, global_key_col, external_id_col, lit(json.dumps(metadata_name_key_to_schema))))
    # Run your UDF, updating the existing uploads column with metadata values
    if metadata_index:
        add_metadata_udf = udf(__add_metadata_udf, upload_schema)
        for column_name in metadata_index:
            table = table.withColumn('uploads', add_metadata_udf(column_name, 'uploads', lit(column_name), lit(json.dumps(metadata_name_key_to_schema)), lit(json.dumps(metadata_index)), lit(divider)))
    return table

def __create_data_rows_udf(row_data_col, global_key_col, external_id_col, metadata_name_key_to_schema_bytes):
    """ Function to-be-wrapped into a pyspark UDF that will create data row dict values (without metadata)
    Args:
        row_data_col                        :   Required (str) - Row data URL column name
        global_key_col                      :   Required (str) - Global Key colunmn name             
        external_id_col                     :   Required (str) - External ID column name
        metadata_name_key_to_schema_bytes   :   Required (bytes) - Bytearray representation of a dictionary where {key=metadata_field_name_key : value=metadata_schema_id}  
    Returns:
        Data row upload value as-a-dictionary with complete key/value pairs for "row_data", "external_id", and "global_key", and an empty list for key "metadata_fields"
    """
    metadata_name_key_to_schema = json.loads(metadata_name_key_to_schema_bytes)
    return {"row_data" : row_data_col, "external_id" : external_id_col, "global_key" : global_key_col, "metadata_fields" : [{"schema_id" : metadata_name_key_to_schema["lb_integration_source"], "value" : "Databricks"}]}  

def __add_metadata_udf(metadata_value_col, data_row_col, metadata_field_name_key, metadata_name_key_to_schema_bytes, metadata_index_bytes, divider):
    """ Function to-be-wrapped into a pyspark UDF that will add a single metadata field / value pair to data row dict metdata lists
    Args:
        metadata_value_col                  :   Required (str) - Metadata value column name
        data_row_col                        :   Required (str) - Data Row Upload column name
        metadata_field_name_key             :   Required (str) - Metadata field name
        metadata_name_key_to_schema_bytes   :   Required (bytes) - Bytearray representation of a converter dictionary where {key=metadata_field_name_key : value=metadata_schema_id}  
        metadata_index_bytes                :   Required (bytes) - Bytearray representation of a converter dictionary where {key=metadata_field_name_key : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
        divider                             :   Required (str) - String delimiter for all name keys generated for parent/child schemas
    Returns:
        Data row upload as-a-dictionary
    """  
    metadata_name_key_to_schema = json.loads(metadata_name_key_to_schema_bytes)
    metadata_type = json.loads(metadata_index_bytes)[str(metadata_field_name_key)]
    if (metadata_value_col is not None) or (str(metadata_value_col) != ""):
        metadata_value_name_key = f"{metadata_field_name_key}{divider}{metadata_value_col}"
        input_metadata_value = metadata_value_col if metadata_type != "enum" else metadata_name_key_to_schema[metadata_value_name_key]
        data_row_col['metadata_fields'].append({"schema_id":metadata_name_key_to_schema[str(metadata_field_name_key)],"value":input_metadata_value})
    return data_row_col

def get_columns_function(table:DataFrame):
    """Grabs all column names from a Pandas DataFrame
    Args:
        spark_table         :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
    Returns:
        List of strings corresponding to all column names
    """
    return [str(col[0]) for col in table.dtypes]

def get_unique_values_function(table:DataFrame, column_name):
    """ Grabs all unique values from a spark table column as strings
    Args:
        spark_table         :   Required (pyspark.sql.dataframe.DataFrame) - Databricks Spark Table object
        column_name         :   Required (str) - Spark Table column name
    Returns:
        List of unique values from a spark table column as strings
    """
    return [str(x.__getitem__(column_name)) for x in table.select(column_name).distinct().collect()]

def add_column_function(table:DataFrame, column_name:str, default_value=""):
    """ Adds a column of empty values to an existing Pandas DataFrame
    Args:
        spark_table         :   Required (pyspark.sql.dataframe.DataFrame) - Databricks Spark Table object
        column_name         :   Required (str) - Spark Table column name
        default_value       :   Optional (str) - Value to insert for every row in the newly created column
    Returns:
        Your table with a new column given the column_name and default_value  
    """
    table = table.withColumn(column_name, lit(default_value))
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

