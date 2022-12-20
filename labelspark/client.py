from labelbox import Client as labelboxClient
from labelbox.schema.data_row_metadata import DataRowMetadata
from labelspark import connector
import pyspark.pandas as pd
from pyspark.sql.functions import lit, col
import json
from datetime import datetime

class Client:
    """ A Databricks Client, containing a Labelbox Client, to-be-run a Databricks notebook
    Args:
        lb_api_key                  :   Required (str) - Labelbox API Key
        lb_endpoint                 :   Optinoal (bool) - Labelbox GraphQL endpoint
        lb_enable_experimental      :   Optional (bool) - If `True` enables experimental Labelbox SDK features
        lb_app_url                  :   Optional (str) - Labelbox web app URL
    Attributes:
        lb_client                   :   labelbox.client.Client object
    Key Functions:
        create_data_rows_from_table :   Creates Labelbox data rows (and metadata) given a Databricks Spark table
        create_table_from_dataset   :   Creates a Databricks Spark table given a Labelbox dataset
        upsert_table_metadata       :   Updates Databricks Spark table metadata columns given a Labelbox dataset
        upsert_labelbox_metadata    :   Updates Labelbox metadata given a Databricks Spark table
    """
    def __init__(
        self, lb_api_key=None, lb_endpoint='https://api.labelbox.com/graphql', 
        lb_enable_experimental=False, lb_app_url="https://app.labelbox.com"
    ):
        self.lb_client = labelboxClient(lb_api_key, endpoint=lb_endpoint, enable_experimental=lb_enable_experimental, app_url=lb_app_url)
    
    def create_data_rows_from_table(
        self, lb_client, spark_table, lb_dataset, row_data_col, global_key_col=None, 
        external_id_col=None, metadata_index={}, skip_duplicates=False, batch_size=20000
    ):
        """ Creates Labelbox data rows given a Spark Table and a Labelbox Dataset. Requires user to specify which columns correspond to which data row values.
        Args:
            lb_client           :   Required( labelbox.client.Client) - Labelbox Client object        
            spark_table         :   Required (pyspark.sql.dataframe.Dataframe) - Spark Table
            lb_dataset          :   Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to add data rows to
            row_data_col        :   Required (str) - Column name for the data row row data URL
            global_key_col      :   Optional (str) - Column name for the global key - defaults to row_data_col
            external_id_col     :   Optional (str) - Column name for the external ID - defaults to global_key_col
            metadata_index      :   Optional (dict) - Determines what metadata gets uploaded to Labelbox - dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
            skip_duplicates     : Optional (bool) - If True, will skip duplicate global_keys, otherwise will generate a unique global_key with a suffix "_1", "_2" and so on
            batch_size          :   Optional (int) - Data row upload batch size - recommended to leave at 20,000
        Returns:
            List of errors from data row upload - if successful, the task results from the upload
        """
        # Assign values to optional arguments
        col_inputs = {
            "row_data_col" : row_data_col, 
            "global_key_col" : global_key_col if global_key_col else row_data_col, 
            "external_id_col" : external_id_col if external_id_col else global_key_col
        }
        # Validate that your named columns exist in your spark_table
        column_names = [str(col[0]) for col in spark_table.dtypes]
        for col_input in col_inputs:
            if col_inputs[col_input] not in column_names:
                print(f'Error: No column matching provided "{col_input}" column value "{col_inputs[col_input]}"')
                return None
        # Sync metadata index keys with metadata ontology
        spark_table = connector.sync_metadata_fields(lb_client=lb_client, spark_table=spark_table, metadata_index=metadata_index)
        if not spark_table:
            return None
        # Create a spark_table with a new "uploads" column that can be queried as a list of dictionaries and uploaded to Labelbox
        starttime = datetime.now()
        uploads_table = connector.create_uploads_column(
            lb_client=lb_client, spark_table=spark_table, 
            row_data_col=col_inputs['row_data_col'], global_key_col=col_inputs['global_key_col'], 
            external_id_col=col_inputs['external_id_col'], metadata_index=metadata_index
        )
        endtime = datetime.now()
        print(f'Success: Labelbox upload conversion complete\n Start Time: {starttime}\n End Time: {endtime}\n Conversion Time: {endtime-starttime}\nData Rows to Upload: {spark_table.count()}')        
        # Query your table and create a dictionary where {key=global_key : value=data_row_dict to-be-uploaded to Labelbox}
        starttime = datetime.now()
        upload_list = uploads_table.select("uploads").rdd.map(lambda x: x.uploads.asDict()).collect()
        global_key_to_upload_dict = {data_row_dict['global_key'] : data_row_dict for data_row_dict in upload_list}
        # Batch upload data rows from your global_key_to_upload_dict
        upload_results = connector.batch_create_data_rows(
            client=lb_client, dataset=lb_dataset, global_key_to_upload_dict=global_key_to_upload_dict, skip_duplicates=skip_duplicates
        )
        endtime = datetime.now()
        if not upload_results:
            print(f'Success: Uploaded table rows to Labelbox dataset with ID {lb_dataset.uid}\n Start Time: {starttime}\n End Time: {endtime}\n Upload Time: {endtime-starttime}\nData rows uploaded: {len(global_key_to_upload_dict)}')
        else:
            print(f'Failure: {len(upload_results)} errors present. Example Error: {upload_results[0]}')
        return upload_results

    def create_table_from_dataset(self, lb_client, lb_dataset, metadata_index={}):
        """ Creates a Spark Table from a Labelbox dataset, optional metadata_fields will create 1 column per field name in list
        Args:
            lb_client           :   Required( labelbox.client.Client) - Labelbox Client object
            lb_dataset          :   Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to create a spark table from
            metadata_index      :   Optional (dict) - Determines what metadata gets uploaded to Labelbox - dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
        Returns:
            Spark Table - pyspark.sql.dataframe.Dataframe objet with columns "data_row_id", "global_key", "external_id", "row_data" and optional metadata fields as columns
        """        
        spark_table = connector.sync_metadata_fields(
            lb_client=lb_client, spark_table=None, metadata_index=metadata_index
        )
        if not spark_table:
            return None
        starttime = datetime.now()
        metadata_fields = list(metadata_index.keys())
        data_rows_list = list(lb_dataset.export_data_rows(include_metadata=True)) if metadata_fields else list(lb_dataset.export_data_rows(include_metadata=False))
        endtime = datetime.now()
        print(f'Labelbox data row export complete\n Start Time: {starttime}\n End Time: {endtime}\n Export Time: {endtime-starttime}\nData rows to create in table: {len(data_rows_list)}')
        starttime = datetime.now()
        metadata_schema_to_name_key = connector.get_metadata_schema_to_name_key(lb_client.get_data_row_metadata_ontology())
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
        spark_table = pd.DataFrame.from_dict(table_dict).to_spark()
        spark_table = spark_table.drop(col("lb_integration_source"))
        endtime = datetime.now()
        print(f'Success: Created table from Labelbox dataset with ID {lb_dataset.uid}\n Start Time: {starttime}\n End Time: {endtime}\n Total Time: {endtime-starttime}\nData rows in table: {len(data_rows_list)}')
        return spark_table
    
    def upsert_table_metadata(self, lb_client, spark_table, global_key_col, global_keys_list=[], metadata_index={}):
        """ Upserts a Spark Table based on the most recent metadata in Labelbox, only upserts columns provided via metadata_fields list
        Args:
            lb_client           :   Required( labelbox.client.Client) - Labelbox Client object            
            spark_table         :   Required (pyspark.sql.dataframe.Dataframe) - Spark Table
            lb_dataset          :   Required (labelbox.schema.dataset.Dataset) - Labelbox dataset to add data rows to
            global_key_col      :   Required (str) - Global key column name to map Labelbox data rows to the Databricks spark table rows
            global_keys_list    :   Optional (list) - List of global keys you wish to upsert - defaults to the whole table
            metadata_index      :   Optional (dict) - Determines what metadata gets upserted in Databricks - dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
        Returns:
            Upserted Spark table
        """
        # Export data row metadata from Labelbox through global keys
        starttime = datetime.now()
        lb_mdo = lb_client.get_data_row_metadata_ontology()
        metadata_schema_to_name_key = connector.get_metadata_schema_to_name_key(lb_mdo)
        ## Either use global_keys provided or all the global keys in the provided global_key column 
        global_keys = global_keys_list if global_keys_list else connector.get_unique_values(spark_table, global_key_col)
        ## Grab data row IDs with global_key list
        data_row_ids = lb_client.get_data_row_ids_for_global_keys(global_keys)['results']
        data_row_id_to_global_key = {str(data_row_ids[i]) : str(global_keys[i]) for i in range(0, len(data_row_ids))}
        ## Get data row metadata with list of data row IDs
        data_row_metadata = lb_mdo.bulk_export(data_row_ids)
        endtime = datetime.now()
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
            spark_table = spark_table.withColumn(metadata_col, upsert_udf(lit(json.dumps(upsert_dict[metadata_col])), global_key_col, metadata_col))
        endtime = datetime.now()
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
        lb_mdo = lb_client.get_data_row_metadata_ontology()
        metadata_schema_to_name_key = connector.get_metadata_schema_to_name_key(lb_mdo)
        metadata_name_key_to_schema = connector.get_metadata_schema_to_name_key(lb_mdo, invert=True)
        ## Either use global_keys provided or all the global keys in the provided global_key column 
        global_keys = global_keys_list if global_keys_list else connector.get_unique_values(spark_table, global_key_col)
        ## Grab data row IDs with global_key list
        data_row_ids = lb_client.get_data_row_ids_for_global_keys(global_keys)['results']
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
