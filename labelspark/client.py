from labelbox import Client as labelboxClient
from labelbox.schema.dataset import Dataset as labelboxDataset
from labelspark.uploader import create_upload_dict
from labelspark.connector import check_pyspark, get_col_names, get_unique_values
from labelbase.connector import validate_columns, determine_actions
from labelbase.uploader import create_global_key_to_data_row_dict, batch_create_data_rows, batch_rows_to_project, batch_upload_annotations
from labelbase.downloader import export_and_flatten_labels
import pyspark

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
    def __init__(self, lb_api_key=None, lb_endpoint='https://api.labelbox.com/graphql', lb_enable_experimental=False, lb_app_url="https://app.labelbox.com"):
        self.lb_client = labelboxClient(lb_api_key, endpoint=lb_endpoint, enable_experimental=lb_enable_experimental, app_url=lb_app_url)
        check_pyspark()
    
    def create_data_rows_from_table(
        self, table:pyspark.sql.dataframe.DataFrame, dataset_id:str="", project_id:str="", priority:int=5, 
        upload_method:str="", skip_duplicates:bool=False, mask_method:str="png", verbose:bool=False, divider="///"):
        """ Creates Labelbox data rows given a Pandas table and a Labelbox Dataset
        Args:
            table               :   Required (pyspark.sql.dataframe.DataFrame) - Spark Table
            dataset_id          :   Required (str) - Labelbox dataset ID to add data rows to - only necessary if no "dataset_id" column exists            
            project_id          :   Required (str) - Labelbox project ID to add data rows to - only necessary if no "project_id" column exists
            priority            :   Optinoal (int) - Between 1 and 5, what priority to give to data row batches sent to projects                             
            upload_method       :   Optional (str) - Either "mal" or "import" - required to upload annotations (otherwise leave as "")
            skip_duplicates     :   Optional (bool) - Determines how to handle if a global key to-be-uploaded is already in use
                                        If True, will skip duplicate global_keys and not upload them
                                        If False, will generate a unique global_key with a suffix {divider} + "1", "2" and so on
            mask_method         :   Optional (str) - Specifies your input mask data format
                                        - "url" means your mask is an accessible URL (must provide color)
                                        - "array" means your mask is a numpy array (must provide color)
                                        - "png" means your mask value is a png-string                                           
            verbose             :   Optional (bool) - If True, prints details about code execution; if False, prints minimal information               
            divider             :   Optional (str) - String delimiter for schema name keys and suffix added to duplocate global keys
        """
        check_pyspark()
        # Create/identify the following values:
            # row_data_col      : column with name "row_data"
            # global_key_col    : column with name "global_key" - defaults to row_data_col
            # external_id_col   : column with name "external_id" - defaults to global_key_col
            # project_id_col    : column with name "project_id" - defaults to ""
            # dataset_id_col    : column with name "dataset_id" - defaults to ""
            # external_id_col   : column with name "external_id" - defaults to global_key_col        
            # metadata_index    : Dictonary where {key=metadata_field_name : value=metadata_type} - defaults to {}
            # attachment_index  : Dictonary where {key=column_name : value=attachment_type} - defaults to {}
            # annotation_index  : Dictonary where {key=column_name : value=top_level_feature_name} - defaults to {}
        row_data_col, global_key_col, external_id_col, project_id_col, dataset_id_col, metadata_index, attachment_index, annotation_index = validate_columns(
            client=self.lb_client, table=table,
            get_columns_function=get_col_names,
            get_unique_values_function=get_unique_values,
            divider=divider, verbose=verbose, extra_client=None
        )
        
        
        # Determine if we're batching and/or uploading annotations
        batch_action, annotate_action = determine_actions(
            dataset_id=dataset_id, dataset_id_col=dataset_id_col, 
            project_id=project_id, project_id_col=project_id_col, 
            upload_method=upload_method, annotation_index=annotation_index
        )
        
        # Create an upload dictionary where {
            # dataset_id : {
                # global_key : {
                    # "data_row" : {}, -- This is your data row upload as a dictionary
                    # "project_id" : "", -- This batches data rows to projects, if applicable
                    # "annotations" : [] -- List of annotations for a given data row, if applicable
                # }
            # }
        # }
        # This uniforms the upload to use labelbase - Labelbox base code for best practices
        upload_dict = create_upload_dict(
            client=self.lb_client, table=table, 
            row_data_col=row_data_col, global_key_col=global_key_col, external_id_col=external_id_col, 
            dataset_id_col=dataset_id_col, dataset_id=dataset_id, 
            project_id_col=project_id_col, project_id=project_id,
            metadata_index=metadata_index, attachment_index=attachment_index, annotation_index=annotation_index,
            upload_method=upload_method, mask_method=mask_method, divider=divider, verbose=verbose
        )      
                
        # Upload your data rows to Labelbox - update upload_dict if global keys are modified during upload
        data_row_upload_results, upload_dict = batch_create_data_rows(
            client=self.lb_client, upload_dict=upload_dict, 
            skip_duplicates=skip_duplicates, divider=divider, verbose=verbose
        )
        
        # Bath to project attempt
        if batch_action: 
            try:
                # Create a dictionary where {key=global_key : value=data_row_id}
                global_keys_list = []
                for dataset_id in upload_dict.keys():
                    for global_key in upload_dict[dataset_id].keys():
                        global_keys_list.append(global_key)
                global_key_to_data_row_id = create_global_key_to_data_row_dict(
                    client=self.lb_client, global_keys=global_keys_list
                )     
                # Create batch dictionary where {key=project_id : value=[data_row_ids]}   
                project_id_to_batch_dict = {}                
                for dataset_id in upload_dict:
                    for global_key in upload_dict[dataset_id].keys():                    
                        project_id = upload_dict[dataset_id][global_key]["project_id"]
                        if project_id:
                            if project_id not in project_id_to_batch_dict.keys():
                                project_id_to_batch_dict[project_id] = []
                            project_id_to_batch_dict[project_id].append(global_key_to_data_row_id[global_key])
                # Labelbase command to batch data rows to projects
                batch_to_project_results = batch_rows_to_project(
                    client=self.lb_client, project_id_to_batch_dict=project_id_to_batch_dict, 
                    priority=priority, verbose=verbose
                )
            except Exception as e:
                annotate_action = False
                batch_to_project_results = e
        else:
            batch_to_project_results = []                
        
        # Annotation upload attempt
        if annotate_action:
            try:               
                # Create batch dictionary where {key=project_id : value=[data_row_ids]}
                project_id_to_upload_dict = {}
                for dataset_id in upload_dict.keys():
                    for global_key in upload_dict[dataset_id].keys():
                        project_id = upload_dict[dataset_id][global_key]["project_id"]
                        annotations_no_data_row_id = upload_dict[dataset_id][global_key]["annotations"]
                        if project_id not in project_id_to_upload_dict.keys():
                            project_id_to_upload_dict[project_id] = []
                        if annotations_no_data_row_id: # For each annotation in the list, add the data row ID and add it to your annotation upload dict
                            for annotation in annotations_no_data_row_id:
                                annotation_data_row_id = annotation
                                annotation_data_row_id["dataRow"] = {"id" : global_key_to_data_row_id[global_key]}
                                project_id_to_upload_dict[project_id].append(annotation_data_row_id)
                # Labelbase command to upload annotations to projects
                annotation_upload_results = batch_upload_annotations(
                    client=self.lb_client, project_id_to_upload_dict=project_id_to_upload_dict, how=upload_method, verbose=verbose
                )
            except Exception as e:
                annotation_upload_results = e
        else:
            annotation_upload_results = []              
            
        return {
            "data_row_upload_results" : data_row_upload_results, 
            "batch_to_project_results" : batch_to_project_results,
            "annotation_upload_results" : annotation_upload_results
        }

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
        table = sync_metadata_fields(
            client=self.lb_client, table=False, get_columns_function=connector.get_columns_function, add_column_function=connector.add_column_function, 
            get_unique_values_function=connector.get_unique_values_function, metadata_index=metadata_index, verbose=verbose
        )                         
        starttime = datetime.now()
        metadata_fields = list(metadata_index.keys())
        data_rows_list = list(lb_dataset.export_data_rows(include_metadata=True)) if metadata_fields else list(lb_dataset.export_data_rows(include_metadata=False))
        endtime = datetime.now()
        if verbose:
            print(f'Labelbox data row export complete\n Start Time: {starttime}\n End Time: {endtime}\n Export Time: {endtime-starttime}\nData rows to create in table: {len(data_rows_list)}')
        starttime = datetime.now()
        metadata_schema_to_name_key = get_metadata_schema_to_name_key(client=self.lb_client)
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
        metadata_schema_to_name_key = get_metadata_schema_to_name_key(client=self.lb_client, lb_mdo=lb_mdo)
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
        metadata_schema_to_name_key = get_metadata_schema_to_name_key(client=self.lb_client, lb_mdo=lb_mdo)
        metadata_name_key_to_schema = get_metadata_schema_to_name_key(client=self.lb_client, lb_mdo=lb_mdo, invert=True)
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
