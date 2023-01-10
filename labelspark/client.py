from labelspark import connector
from pyspark.sql.dataframe import DataFrame
from labelbox import Client as labelboxClient
from labelbase import Client as labelbaseClient
from labelbox.schema.dataset import Dataset as labelboxDataset
from labelbox.schema.data_row_metadata import DataRowMetadata
from pyspark.sql.functions import lit, col
import json
from datetime import datetime

try:
  import pyspark.pandas as pd
  needs_koalas = False
except:
  import databricks.koalas as pd
  needs_koalas = True 

class Client:
    """ A Databricks Client, containing a Labelbox Client, to-be-run a Databricks notebook
    Args:
        lb_api_key                  :   Required (str) - Labelbox API Key
        lb_endpoint                 :   Optinoal (bool) - Labelbox GraphQL endpoint
        lb_enable_experimental      :   Optional (bool) - If `True` enables experimental Labelbox SDK features
        lb_app_url                  :   Optional (str) - Labelbox web app URL
    Attributes:
        lb_client                   :   labelbox.client.Client object
        base_client                 :   labelbase.Client object
    Key Functions:
        create_data_rows_from_table :   Creates Labelbox data rows (and metadata) given a Databricks Spark table
        create_table_from_dataset   :   Creates a Databricks Spark table given a Labelbox dataset
        upsert_table_metadata       :   Updates Databricks Spark table metadata columns given a Labelbox dataset
        upsert_labelbox_metadata    :   Updates Labelbox metadata given a Databricks Spark table
    """
    def __init__(self, lb_api_key=None, lb_endpoint='https://api.labelbox.com/graphql', lb_enable_experimental=False, lb_app_url="https://app.labelbox.com"):
        self.lb_client = labelboxClient(lb_api_key, endpoint=lb_endpoint, enable_experimental=lb_enable_experimental, app_url=lb_app_url)
        self.base_client = labelbaseClient(lb_api_key, lb_endpoint=lb_endpoint, lb_enable_experimental=lb_enable_experimental, lb_app_url=lb_app_url)
        connector.check_pyspark()
    
    def create_data_rows_from_table(
        self, table:DataFrame, lb_dataset:labelboxDataset, row_data_col:str, global_key_col:str="", external_id_col:str="",
        metadata_index:dict={}, local_files:bool=False, skip_duplicates:bool=False, verbose:bool=False, divider="___"):
        """ Creates Labelbox data rows given a Pandas table and a Labelbox Dataset
        Args:
            table           :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
            lb_dataset      :   Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to add data rows to            
            row_data_col    :   Required (str) - Column containing asset URL or file path
            local_files     :   Required (bool) - Determines how to handle row_data_col values
                                    If True, treats row_data_col values as file paths uploads the local files to Labelbox
                                    If False, treats row_data_col values as urls (assuming delegated access is set up)
            global_key_col  :   Optional (str) - Column name containing the data row global key - defaults to row_data_col
            external_id_col :   Optional (str) - Column name containing the data row external ID - defaults to global_key_col
            metadata_index  :   Required (dict) - Dictionary where {key=column_name : value=metadata_type}
                                    metadata_type must be either "enum", "string", "datetime" or "number"
            skip_duplicates :   Optional (bool) - Determines how to handle if a global key to-be-uploaded is already in use
                                    If True, will skip duplicate global_keys and not upload them
                                    If False, will generate a unique global_key with a suffix "_1", "_2" and so on
            verbose         :   Required (bool) - If True, prints details about code execution; if False, prints minimal information
            divider         :   Optional (str) - String delimiter for all name keys generated for parent/child schemas
        Returns:
            A dictionary with "upload_results" and "conversion_errors" keys
            - "upload_results" key pertains to the results of the data row upload itself
            - "conversion_errors" key pertains to any errors related to data row conversion
        """    
        
        # Ensure all your metadata_index keys are metadata fields in Labelbox and that your Pandas DataFrame has all the right columns
        table = self.base_client.sync_metadata_fields(
            table=table, get_columns_function=connector.get_columns_function, add_column_function=connector.add_column_function, 
            get_unique_values_function=connector.get_unique_values_function, metadata_index=metadata_index, verbose=verbose
        )
        
        # If df returns False, the sync failed - terminate the upload
        if type(table) == bool:
            return {"upload_results" : [], "conversion_errors" : []}
        
        # Create a dictionary where {key=global_key : value=labelbox_upload_dictionary} - this is unique to Pandas
        global_key_to_upload_dict, conversion_errors = connector.create_upload_dict(
            table=table, lb_client=self.lb_client, base_client=self.base_client,
            row_data_col=row_data_col, global_key_col=global_key_col, external_id_col=external_id_col, 
            metadata_index=metadata_index, local_files=local_files, divider=divider, verbose=verbose
        )
        
        # If there are conversion errors, let the user know; if there are no successful conversions, terminate the upload
        if conversion_errors:
            print(f'There were {len(conversion_errors)} errors in creating your upload list - see result["conversion_errors"] for more information')
            if global_key_to_upload_dict:
                print(f'Data row upload will continue')
            else:
                print(f'Data row upload will not continue')  
                return {"upload_results" : [], "conversion_errors" : errors}
                
        # Upload your data rows to Labelbox
        upload_results = self.base_client.batch_create_data_rows(
            dataset=lb_dataset, global_key_to_upload_dict=global_key_to_upload_dict, 
            skip_duplicates=skip_duplicates, divider=divider, verbose=verbose
        )
        
        return {"upload_results" : upload_results, "conversion_errors" : conversion_errors}

    def create_table_from_dataset(self, lb_dataset:labelboxDataset, metadata_index:dict={}, verbose:bool=False, divider:str="///"):
        """ Creates a Spark Table from a Labelbox dataset, optional metadata_fields will create 1 column per field name in list
        Args:
            lb_dataset      :   Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to create a spark table from
            metadata_index  :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type}
                                    metadata_type must be either "enum", "string", "datetime" or "number"
            verbose         :   Required (bool) - If True, prints details about code execution; if False, prints minimal information
            divider         :   Optional (str) - String delimiter for all name keys generated for parent/child schemas            
        Returns:
            Spark Table - pyspark.sql.dataframe.Dataframe objet with columns "data_row_id", "global_key", "external_id", "row_data" and optional metadata fields as columns
        """        
        table = self.base_client.sync_metadata_fields(
            table=False, get_columns_function=connector.get_columns_function, add_column_function=connector.add_column_function, 
            get_unique_values_function=connector.get_unique_values_function, metadata_index=metadata_index, verbose=verbose
        )                         
        starttime = datetime.now()
        metadata_fields = list(metadata_index.keys())
        data_rows_list = list(lb_dataset.export_data_rows(include_metadata=True)) if metadata_fields else list(lb_dataset.export_data_rows(include_metadata=False))
        endtime = datetime.now()
        if verbose:
            print(f'Labelbox data row export complete\n Start Time: {starttime}\n End Time: {endtime}\n Export Time: {endtime-starttime}\nData rows to create in table: {len(data_rows_list)}')
        starttime = datetime.now()
        metadata_schema_to_name_key = self.base_client.get_metadata_schema_to_name_key()
        # Create empty dict where {key= column name : value= list of values for column} (in order)
        table_dict = {"data_row_id" : [], "global_key" : [], "external_id" : [], "row_data" : []}
        if metadata_fields:
            for column_name in metadata_fields:
                table_dict[column_name] = []
        # Append to your dict where {key= column name : value= list of values for column} (in order)
        for data_row in data_rows_list:
            # Create default column values
            table_dict['data_row_id'].append(data_row.uid)
            table_dict['global_key'].append(data_row.global_key)
            table_dict['external_id'].append(data_row.external_id)
            table_dict['row_data'].append(data_row.row_data)
            # If there's metadata, add it, with default value as None
            if metadata_fields:
                metadata_dict = {metadata_name : None for metadata_name in metadata_fields}
                if data_row.fields:
                    for metadata_field in data_row.metadata_fields:
                        if metadata_field['name'] in metadata_dict:
                            # If a metatata value's string is a schemaId, it's an enum option we need to convert. Otherwise, we can take the value itself.
                            metadata_dict[metadata_field['name']] = metadata_schema_to_name_key[metadata_field['value']].split("///")[1] if metadata_field['value'] in metadata_schema_to_name_key else metadata_field['value']
                for metadata_name in metadata_dict:
                    table_dict[metadata_name].append(metadata_dict[metadata_name])
        table = pd.DataFrame.from_dict(table_dict).to_spark()
        table = table.drop(col("lb_integration_source"))
        endtime = datetime.now()
        if verbose:
            print(f'Success: Created table from Labelbox dataset with ID {lb_dataset.uid}\n Start Time: {starttime}\n End Time: {endtime}\n Total Time: {endtime-starttime}\nData rows in table: {len(data_rows_list)}')
        return table
    
    def upsert_table_metadata(self, table, global_key_col, global_keys_list=[], metadata_index={}, verbose:bool=False):
        """ Upserts a Spark Table based on the most recent metadata in Labelbox, only upserts columns provided via metadata_fields list
        Args:
            table           :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
            global_key_col  :   Required (str) - Column name containing the data row global key
            global_keys_list:   Optional (list) - List of global keys you wish to upsert - defaults to the whole table
            metadata_index  :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type}
                                    metadata_type must be either "enum", "string", "datetime" or "number"
        Returns:
            Upserted Spark table
        """
        # Export data row metadata from Labelbox through global keys
        starttime = datetime.now()
        lb_mdo = self.lb_client.get_data_row_metadata_ontology()
        metadata_schema_to_name_key = self.base_client.get_metadata_schema_to_name_key(lb_mdo=lb_mdo)
        ## Either use global_keys provided or all the global keys in the provided global_key column 
        global_keys = global_keys_list if global_keys_list else connector.get_unique_values(table, global_key_col)
        ## Grab data row IDs with global_key list
        data_row_ids = self.lb_client.get_data_row_ids_for_global_keys(global_keys)['results']
        data_row_id_to_global_key = {str(data_row_ids[i]) : str(global_keys[i]) for i in range(0, len(data_row_ids))}
        ## Get data row metadata with list of data row IDs
        data_row_metadata = lb_mdo.bulk_export(data_row_ids)
        endtime = datetime.now()
        if verbose:
            print(f'Labelbox metadata export complete\n Start Time: {starttime}\n End Time: {endtime}\n Export Time: {endtime-starttime}\nData rows in export: {len(data_row_metadata)}')        
        # Run the table through a UDF that will update metadata column values using the Global Key as the primary key        
        starttime = datetime.now()
        ## Create a dict where {metadata_field_name (column name) : {global_key : new_metadata_value}} (for enums, is the schema ID)
        metadata_fields = list(metadata_index.keys())
        upsert_dict = {}
        for data_row in data_row_metadata:
            global_key = str(data_row_id_to_global_key[str(data_row.data_row_id)])
            for field in data_row.fields:
                if field.schema_id in metadata_schema_to_name_key:
                    metadata_col = metadata_schema_to_name_key[field.schema_id]
                    if metadata_col in metadata_fields:
                        if metadata_col not in upsert_dict.keys():
                            upsert_dict[metadata_col] = {}
                        input_value = metadata_schema_to_name_key[field.value].split("///")[1] if field.value in metadata_schema_to_name_key.keys() else field.value
                        upsert_dict[metadata_col][global_key] = input_value
        ## For each metadata field column, use a UDF upsert column values with the values in your dict where {key=global_key : value=new_metadata_value}
        upsert_udf = connector.column_upsert_udf()
        for metadata_col in upsert_dict:
            table = table.withColumn(metadata_col, upsert_udf(lit(json.dumps(upsert_dict[metadata_col])), global_key_col, metadata_col))
        endtime = datetime.now()
        if verbose:
            print(f'Upsert table metadata complete\n Start Time: {starttime}\n End Time: {endtime}\n Total Time: {endtime-starttime}\nData rows upserted: {len(data_row_metadata)}') 
        return spark_table

    def upsert_labelbox_metadata(self, lb_client, spark_table, global_key_col, global_keys_list=[], metadata_index={}):
        """ Updates Labelbox data row metadata based on the most recent metadata from a Databricks spark table, only updates metadata fields provided via a metadata_index keys
        Args:
            lb_client           :   Required( labelbox.client.Client) - Labelbox Client object                    
            spark_table         :   Required (pyspark.sql.dataframe.Dataframe) - Spark Table
            lb_dataset          :   Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to add data rows to
            global_key_col      :   Required (str) - Global key column name to map Labelbox data rows to the Databricks spark table rows
            global_keys_list    :   Optional (list) - List of global keys you wish to upsert - defaults to the whole table
            metadata_index      :   Optional (dict) - Determines what metadata gets upserted in Labelbox - dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
        Returns:
            List of errors from metadata ontology bulk upsert - if successful, is an empty list
        """
        # Export data row metadata from Labelbox through global keys        
        starttime = datetime.now()
        lb_mdo = self.lb_client.get_data_row_metadata_ontology()
        metadata_schema_to_name_key = self.base_client.get_metadata_schema_to_name_key(lb_mdo=lb_mdo)
        metadata_name_key_to_schema = self.base_client.get_metadata_schema_to_name_key(lb_mdo=lb_mdo, invert=True)
        ## Either use global_keys provided or all the global keys in the provided global_key column 
        global_keys = global_keys_list if global_keys_list else connector.get_unique_values(spark_table, global_key_col)
        ## Grab data row IDs with global_key list
        data_row_ids = self.lb_client.get_data_row_ids_for_global_keys(global_keys)['results']
        data_row_id_to_global_key = {str(data_row_ids[i]) : str(global_keys[i]) for i in range(0, len(data_row_ids))}
        ## Get data row metadata with list of data row IDs
        data_row_metadata = lb_mdo.bulk_export(data_row_ids)  
        endtime = datetime.now()
        print(f'Laeblbox metadata export complete\n Start Time: {starttime}\n End Time: {endtime}\n Export Time: {endtime-starttime}\nData rows in export: {len(data_row_metadata)}')                
        # Structure a new metadata upload by grabbing the latest values from your spark table, using the global key as the primary key
        starttime = datetime.now()
        metadata_fields = list(metadata_index.keys())
        upload_metadata = []  
        for data_row in data_row_metadata:
            global_key = data_row_id_to_global_key[str(data_row.data_row_id)]
            new_metadata = data_row.fields[:]
            for field in new_metadata:
                field_name = metadata_schema_to_name_key[field.schema_id]
                if field_name in metadata_fields:
                    table_value = spark_table.filter(f"{global_key_col} = '{global_key}'").collect()[0].__getitem__(field_name)
                    table_name_key = f"{field_name}///{table_value}"
                    field.value = metadata_name_key_to_schema[table_name_key] if table_name_key in metadata_name_key_to_schema.keys() else table_value
            upload_metadata.append(DataRowMetadata(data_row_id=data_row.data_row_id, fields=new_metadata))
        results = lb_mdo.bulk_upsert(upload_metadata)
        endtime = datetime.now()
        print(f'Upsert Labelbox metadata complete\n Start Time: {starttime}\n End Time: {endtime}\n Total Time: {endtime-starttime}\nData rows upserted: {len(upload_metadata)}')         
        return results
