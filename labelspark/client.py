import pyspark
from labelbox import Client as labelboxClient
from labelspark.uploader import create_upload_dict
from labelspark.connector import check_pyspark, get_col_names, get_unique_values
from labelbase.connector import validate_columns, determine_actions
from labelbase.uploader import create_global_key_to_data_row_id_dict, batch_create_data_rows, batch_rows_to_project, batch_upload_annotations
from labelbase.downloader import export_and_flatten_labels
import json
from delta import *
from uuid import uuid4
from labelbase.downloader import *
from labelbase.uploader import *
from labelbase.connector import *
from labelspark import connector, uploader

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
        
    def export_to_table(
        self, project, 
        include_metadata:bool=False, include_performance:bool=False, include_agreement:bool=False,
        verbose:bool=False, mask_method:str="png", divider="///"):
        """ Creates a Spark DataFrame given a Labelbox Projet ID
        Args:
            project                 :   Required (str / lablebox.Project) - Labelbox Project ID or lablebox.Project object to export labels from
            include_metadata        :   Optional (bool) - If included, exports metadata fields
            include_performance     :   Optional (bool) - If included, exports labeling performance
            include_agreement       :   Optional (bool) - If included, exports consensus scores       
            verbose                 :   Optional (bool) - If True, prints details about code execution; if False, prints minimal information
            mask_method             :   Optional (str) - Specifies your desired mask data format
                                            - "url" leaves masks as-is
                                            - "array" converts URLs to numpy arrays
                                            - "png" converts URLs to png byte strings             
            divider                 :   Optional (str) - String delimiter for schema name keys and suffix added to duplocate global keys 
        """
        spark = pyspark.sql.SparkSession.builder.appName('labelspark_export').getOrCreate()  
        sc = spark.sparkContext
        
        flattened_labels_dict = export_and_flatten_labels(
            client=self.lb_client, project=project, 
            include_metadata=include_metadata, include_performance=include_performance, include_agreement=include_agreement,
            mask_method=mask_method, verbose=verbose, divider=divider
        )

        for label in flattened_labels_dict:
            for key in label.keys():
                if divider in key:
                    label[key] = json.dumps(label[key])

    
        df = spark.createDataFrame(sc.parallelize(flattened_labels_dict))
        
        if verbose:
            print(f"Success: DataFrame generated")
        
        return df        
    
    def export_to_delta_table(self, project, 
        include_metadata:bool=False, include_performance:bool=False, include_agreement:bool=False,
        verbose:bool=False, mask_method:str="png", divider="///", table_path="", write_mode="overwrite", spark:pyspark.sql.SparkSession=None, spark_config:dict={}):
        """ Creates a Spark Dataframe and saves it to a Databricks Delta Table
        Args:
            project                 :   Required (str / lablebox.Project) - Labelbox Project ID or lablebox.Project object to export labels from
            include_metadata        :   Optional (bool) - If included, exports metadata fields
            include_performance     :   Optional (bool) - If included, exports labeling performance
            include_agreement       :   Optional (bool) - If included, exports consensus scores       
            verbose                 :   Optional (bool) - If True, prints details about code execution; if False, prints minimal information
            mask_method             :   Optional (str) - Specifies your desired mask data format
                                            - "url" leaves masks as-is
                                            - "array" converts URLs to numpy arrays
                                            - "png" converts URLs to png byte strings             
            divider                 :   Optional (str) - String delimiter for schema name keys and suffix added to duplicate global keys
            table_path              :   Required (str) - Path where the delta table will be saved
                                            - To save the delta table to a local folder, provide an absolute path
                                            - To save the delta table to a GCS bucket, provide a gs link with the format gs://{project}/{dataset}/{folder}
                                            - To save the delta table to an S3 bucket, provide an S3 link with the format s3a:/{bucket}/{folder}/{table_name}
            write_mode              :   Required (str) - used for writing Dataframe to Delta Table, must be either 'append' or 'overwrite'
            spark                   :   Optional (pyspark.sql.SparkSession) - SparkSession to use for exporting the project to a Delta Table, if not provided a spark session will
                                                                              be created based on the spark_config arg. This argument is mostly intended for debugging issues related to
                                                                              the required jar files for AWS/GCS connectors
            spark_config            :   Optional (dict) - spark_config needs to be in the following formats for uploading to AWS S3/GCS
                                            - S3: 
                                                {
                                                    "jars": [<path to hadoop-aws-{version}.jar file>, <path to aws-java-sdk-bundle-{version}jar file>],
                                                    "AWS_ACCESS_KEY": <AWS access key>,
                                                    "AWS_SECRET_KEY": <AWS secret key>
                                                }
                                                
                                            - GCS:
                                                {
                                                    "jars": [<path to gcs-connector-hadoop2-latest.jar file>],
                                                    "credentials": <path to GCS service account json keyfile>
                                                }"""

        if spark is None:
            spark = self.get_spark_session(table_path, spark_config)
        sc = spark.sparkContext
        
        flattened_labels_dict = export_and_flatten_labels(
            client=self.lb_client, project=project, 
            include_metadata=include_metadata, include_performance=include_performance, include_agreement=include_agreement,
            mask_method=mask_method, verbose=verbose, divider=divider
        )

        for label in flattened_labels_dict:
            for key in label.keys():
                if divider in key:
                    label[key] = json.dumps(label[key])
                elif label[key] == None:
                    label[key] = ""

        
        df = spark.createDataFrame(sc.parallelize(flattened_labels_dict))
        df.write.format("delta").mode(write_mode).save(table_path)
        
        if verbose:
            print(f"Success: Table saved to {table_path}")
        
        return df
    
    def get_spark_session(self, save_path, spark_config):
        if save_path[:5] == 'gs://':
            if 'credentials' not in spark_config.keys():
                raise ValueError(f"argument spark_config must contain a 'credentials' key to upload to GCS")
            if 'jars' not in spark_config.keys():
                raise ValueError(f"argument spark_config must contain a 'jars' key containing the path to the gcs-connector-hadoop2-latest.jar file to connect to GCS")
            if type(spark_config['jars']) == str:
                jar_file = spark_config['jars']
            elif type(spark_config['jar']) == list:
                jar_file = ''
                for file in spark_config['jar']:
                    jar_file += file + ', '
                jar_file = jar_file[:-2]
            builder = pyspark.sql.SparkSession.builder.appName("labelspark_export").config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.jars", jar_file)
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            spark.conf.set("spark.hadoop.fs.gs.auth.service.account.enable", "true")   
            spark.conf.set("google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
            spark.conf.set('google.cloud.auth.service.account.json.keyfile', spark_config['credentials'])
            spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            spark.conf.set("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
            spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
        elif save_path[:5] == 's3a:/':
            if 'AWS_ACCESS_KEY' not in spark_config.keys() or 'AWS_SECRET_KEY' not in spark_config.keys():
                raise ValueError(f"argument spark_config must contain 'AWS_ACCESS_KEY' and 'AWS_SECRET_KEY' as keys to connect to S3")
            if 'jars' not in spark_config.keys():
                raise ValueError("argument spark_config must contain a 'jars' key with a list of strings containing the paths to hadoop-aws-{version}.jar and aws-java-sdk-bundle-{version}jar files")
            if type(spark_config['jars']) == str:
                jar_file = spark_config['jars']
            elif type(spark_config['jar']) == list:
                jar_file = ''
                for file in spark_config['jar']:
                    jar_file += file + ', '
                jar_file = jar_file[:-2]
            builder = pyspark.sql.SparkSession.builder.appName("labelspark_export").config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.jars", jar_file)
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            sc = spark.sparkContext
            hadoop_conf=sc._jsc.hadoopConfiguration()
            hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            hadoop_conf.set("fs.s3a.access.key", spark_config['AWS_ACCESS_KEY'])
            hadoop_conf.set("fs.s3a.secret.key", spark_config['AWS_SECRET_KEY'])
            sc._conf.setAll([('spark.delta.logStore.class','org.apache.spark.sql.delta.storage.S3SingleDriverLogStore')])
            spark.sparkContext._conf.getAll()

        else:
            builder = pyspark.sql.SparkSession.builder.appName("labelspark_export").config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark
    
    def create_data_rows_from_table(
        self, table:pyspark.sql.dataframe.DataFrame, dataset_id:str="", project_id:str="", priority:int=5, 
        upload_method:str="", skip_duplicates:bool=False, model_id="", model_run_id="", mask_method:str="png", verbose:bool=False, divider="///"):
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
        # Create/identify the following values:
            # row_data_col      : column with name "row_data"
            # global_key_col    : column with name "global_key" - defaults to row_data_col
            # external_id_col   : column with name "external_id" - defaults to global_key_col
            # project_id_col    : column with name "project_id" - defaults to ""
            # dataset_id_col    : column with name "dataset_id" - defaults to ""
            # metadata_index    : Dictonary where {key=metadata_field_name : value=metadata_type} - defaults to {}
            # attachment_index  : Dictonary where {key=column_name : value=attachment_type} - defaults to {}
            # annotation_index  : Dictonary where {key=column_name : value=top_level_feature_name} - defaults to {}
        x = validate_columns(
            client=self.lb_client, table=table,
            get_columns_function=get_col_names,
            get_unique_values_function=get_unique_values,
            divider=divider, verbose=verbose, extra_client=None
        )
                
        # Determine if we're batching and/or uploading annotations
        actions = determine_actions(
            row_data_col=x["row_data_col"], dataset_id=dataset_id, dataset_id_col=x["dataset_id_col"], project_id=project_id, 
            project_id_col=x["project_id_col"], model_id=model_id, model_id_col=x["model_id_col"], model_run_id=model_run_id, 
            model_run_id_col=x["model_run_id_col"], upload_method=upload_method, metadata_index=x["metadata_index"], 
            attachment_index=x["attachment_index"], annotation_index=x["annotation_index"], prediction_index=x["prediction_index"]
        )
        if not actions["create"]:
            raise ValueError(f"No `dataset_id` argument or `dataset_id` column was provided in table to create data rows")
        
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
            row_data_col=x["row_data_col"], global_key_col=x["global_key_col"], external_id_col=x["external_id_col"], 
            dataset_id_col=x["dataset_id_col"], dataset_id=dataset_id, 
            project_id_col=x["project_id_col"], project_id=project_id,
            metadata_index=x["metadata_index"], attachment_index=x["attachment_index"], 
            annotation_index=x["annotation_index"],
            upload_method=upload_method, mask_method=mask_method, divider=divider, verbose=verbose
        )    
                
        # Upload your data rows to Labelbox - update upload_dict if global keys are modified during upload
        data_row_upload_results, upload_dict = batch_create_data_rows(
            client=self.lb_client, upload_dict=upload_dict, 
            skip_duplicates=skip_duplicates, divider=divider, verbose=verbose
        )
        
        # Bath to project attempt
        if actions['batch']: 
            try:
                # Create a dictionary where {key=global_key : value=data_row_id}
                global_keys_list = []
                for dataset_id in upload_dict.keys():
                    for global_key in upload_dict[dataset_id].keys():
                        global_keys_list.append(global_key)
                global_key_to_data_row_id = create_global_key_to_data_row_id_dict(
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
                actions['annotate'] = False
                batch_to_project_results = e
        else:
            batch_to_project_results = []                
        
        # Annotation upload attempt
        if actions['annotate']:
            try:               
                # Create batch dictionary where {key=project_id : value=[data_row_ids]}
                project_id_to_upload_dict = {}
                for dataset_id in upload_dict.keys():
                    for global_key in upload_dict[dataset_id].keys():
                        project_id = upload_dict[dataset_id][global_key]["project_id"]
                        annotations = []
                        annotations_no_data_row_id = upload_dict[dataset_id][global_key]["annotations"]
                        for annotation in annotations_no_data_row_id:
                            for key in annotation.keys():
                                annotations.append(annotation[key])
                        annotations_no_data_row_id = annotations
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
    
    def create_data_rows_from_delta_table(self, dataset_id:str="", project_id:str="", priority:int=5, 
        upload_method:str="", skip_duplicates:bool=False, mask_method:str="png", verbose:bool=False, divider="///", table_path="", spark:pyspark.sql.SparkSession=None, spark_config:dict={}):
        
        if spark is None:
            spark = self.get_spark_session(table_path, spark_config)

        df = spark.read.load(table_path)
        return(self.create_data_rows_from_table(table=df, dataset_id=dataset_id, project_id=project_id, priority=priority, upload_method=upload_method, skip_duplicates=skip_duplicates, mask_method=mask_method, verbose=verbose, divider=divider))

    def upsert_data_rows_from_table(self, table:pyspark.sql.dataframe.DataFrame, dataset_id:str="", project_id:str="", model_id:str="", upload_method:str="", mask_method:str="png", priority:int=5, model_run_id:str="", batch_data_rows:bool=False, annotation_method:str="", verbose:bool=False, divider:str="///"):
        """ Performs the following actions if proper information is provided:
                - batches data rows to projects (if batch_data_rows == True) * **
                - uploads annotations as pre-labels or submitted labels * **
                - sends submitted labels to model runs as ground truth labels * ** ***
                - uploads predictions to a model run * ***
            * Requires a global_key AND dataset_id OR dataset_id_col
            ** Requires project_id OR project_id_col
            *** Requires model_id OR model_id_col OR model_run_id OR model_run_id_col      
        Args:
            batch_data_rows     :   Optional (bool) - If True, will batch data rows to projects if not already in projects
            anootation_method   :   Optional (bool) -  Must be one of the following annotation import methods:
                                            "" - Does nothing with annotations
                                            "mal" - Uploaded to project as pre-labels
                                            "import" - Uploaded to project assubmitted labels
                                            "ground-truth" - Project submitted labels AND/OR ground truth submitted labels (will check existing project labels first)
            upsert_metadata     :   True if upserting metadata, False if not
            upsert_attachments  :   True if upserting attachments, False if not
        Returns:
            Results from all performed actions in a dictionary - if an expected action has no results, it was not performed
        """
        # Confirm the existience of specific columns and construct reference indexes for different kinds of data to-be-uploaded using labelbase.connector.validate_columns
            # row_data_col                :   Column representing asset URL, raw text, or path to local asset file
            # global_key_col              :   Defaults to row_data_col
            # external_id_col             :   Defaults to global_key_col
            # project_id_col              :   Either `project_id` or "" - overridden by arg for project_id        
            # dataset_id_col              :   Either `dataset_id` or "" - overridden by arg for dataset_id
            # model_id_col                :   Either `model_id` or "" - overridden by arg for model_id and any args for model_run_id
            # model_run_id_col            :   Either `model_run_id` or "" - overridden by arg for model_run_id        
            # metadata_index              :   Dictonary where {key=metadata_field_name : value=metadata_type} or {} if not uploading metadata
            # attachment_index            :   Dictonary where {key=column_name : value=attachment_type} or {} if not uploading attachments
            # annotation_index            :   Dictonary where {key=column_name : value=top_level_class_name} or {} if not uploading annotations
            # prediction_index            :   Dictonary where {key=column_name : value=top_level_class_name} or {}  if not uploading predictions
        
        x = validate_columns(
            client=self.lb_client, table=table,
            get_columns_function=connector.get_col_names,
            get_unique_values_function=connector.get_unique_values,
            divider=divider, verbose=verbose, extra_client=None
        )
        
        # Determine the actions we're taking in this run using labelbase.connector.determine_actions
        actions = determine_actions(
            row_data_col=x["row_data_col"], dataset_id=dataset_id, dataset_id_col=x["dataset_id_col"], project_id=project_id, 
            project_id_col=x["project_id_col"], model_id=model_id, model_id_col=x["model_id_col"], model_run_id=model_run_id, 
            model_run_id_col=x["model_run_id_col"], upload_method=upload_method, metadata_index=x["metadata_index"], 
            attachment_index=x["attachment_index"], annotation_index=x["annotation_index"], prediction_index=x["prediction_index"]
        )             
        
        # Create an upload dictionary where:
        # {
            # global_key : {
                # "project_id" : "", -- This batches data rows to projects, if applicable
                # "annotations" : [], -- List of annotations for a given data row, if applicable
                # "model_run_id" : "", -- This adds data rows to model runs, if applicable
                # "predictions" : [] -- List of predictions for a given data row, if applicable
            # }
        # }
        # This uniforms the upload to use labelbase - Labelbox base code for best practices
        upload_dict = uploader.create_upload_dict( # Using labelpandas.uploader.create_upload_dict
            client=self.lb_client, table=table, 
            row_data_col=x["row_data_col"], global_key_col=x["global_key_col"], external_id_col=x["external_id_col"], 
            dataset_id_col=x["dataset_id_col"], dataset_id=dataset_id, 
            project_id_col=x["project_id_col"], project_id=project_id,
            metadata_index=x["metadata_index"], attachment_index=x["attachment_index"], 
            annotation_index=x["annotation_index"], upload_method=upload_method, 
            mask_method=mask_method, divider=divider, verbose=verbose
        )
        for gk in upload_dict.keys():
            annotations_filtered = []
            annotations = upload_dict[gk]['annotations']
            for annotation in annotations:
                for key in annotation.keys():
                    annotations_filtered.append(annotation[key])
            upload_dict[gk]['annotations'] = annotations_filtered

        # Bath to project attempt using labelbase.uploader.batch_rows_to_project
        if actions["batch"] and batch_data_rows:         
            batch_to_project_results = batch_rows_to_project(
                client=self.lb_client, upload_dict=upload_dict, priority=priority, verbose=verbose
            )                
        else:
            batch_to_project_results = []

        # Annotation upload attempt using labelbase.uploader.batch_upload_annotations
        if actions["annotate"]:
            annotation_upload_results = batch_upload_annotations(
                client=self.lb_client, upload_dict=upload_dict, how=actions["annotate"], verbose=verbose
            )
        else:
            annotation_upload_results = []

        # If model run upload attempt labelbase.uploader.batch_add_ground_truth_to_model_run
        if actions["annotate"] == "ground-truth":
            ground_truth_upload_results = batch_add_ground_truth_to_model_run(
                client=self.lb_client, upload_dict=upload_dict
            )
        else:
            ground_truth_upload_results = []
            
        # Prediction upload attempt using labelbase.uploader.batch_upload_predictions
        if actions["predictions"]:
            # If data rows aren't in the model run yet, add them with labelbase.uploader.batch_add_data_rows_to_model_run
            if not ground_truth_upload_results:
                data_row_to_model_run_results = batch_add_data_rows_to_model_run(
                    client=self.lb_client, upload_dict=upload_dict                    
                )
            prediction_upload_results = batch_upload_predictions(
                client=self.lb_client, upload_dict=upload_dict
            )
        else:
            data_row_to_model_run_results = []
            prediction_upload_results = []  

        return_payload = {
            "batch_to_project_results" : batch_to_project_results,
            "annotation_upload_results" : annotation_upload_results,
            "ground_truth_upload_results" : ground_truth_upload_results,
            "data_row_to_model_run_results" : data_row_to_model_run_results,
            "prediction_upload_results" : prediction_upload_results
        }             

        return_payload = {k: v for k, v in return_payload.items() if v}
            
        return return_payload

    def upsert_data_rows_from_delta_table(self, dataset_id:str="", project_id:str="", model_id:str="", upload_method:str="", mask_method:str="png", priority:int=5, model_run_id:str="", batch_data_rows:bool=False, annotation_method:str="", verbose:bool=False, divider:str="///", table_path:str="", spark:pyspark.sql.SparkSession=None, spark_config:dict={}):
        """ Performs the following actions if proper information is provided:
                - batches data rows to projects (if batch_data_rows == True) * **
                - uploads annotations as pre-labels or submitted labels * **
                - sends submitted labels to model runs as ground truth labels * ** ***
                - uploads predictions to a model run * ***
            * Requires a global_key AND dataset_id OR dataset_id_col
            ** Requires project_id OR project_id_col
            *** Requires model_id OR model_id_col OR model_run_id OR model_run_id_col      
        Args:
            batch_data_rows     :   Optional (bool) - If True, will batch data rows to projects if not already in projects
            anootation_method   :   Optional (bool) -  Must be one of the following annotation import methods:
                                            "" - Does nothing with annotations
                                            "mal" - Uploaded to project as pre-labels
                                            "import" - Uploaded to project assubmitted labels
                                            "ground-truth" - Project submitted labels AND/OR ground truth submitted labels (will check existing project labels first)
            upsert_metadata     :   True if upserting metadata, False if not
            upsert_attachments  :   True if upserting attachments, False if not
        Returns:
            Results from all performed actions in a dictionary - if an expected action has no results, it was not performed
        """
        if spark is None:
            spark = self.get_spark_session(table_path, spark_config)

        df = spark.read.load(table_path)
        return self.upsert_data_rows_from_table(df, dataset_id, project_id, model_id, upload_method, mask_method, priority, model_run_id, batch_data_rows, annotation_method, verbose, divider)