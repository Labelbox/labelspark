"""
uploader.py holds the function create_upload_dict() -- which creates the following style dictionary:
{
    dataset_id : {
        global_key : {
            "data_row" : {}, -- This is your data row upload as a dictionary
            "project_id" : "", -- This batches data rows to projects, if applicable
            "annotations" : [] -- List of annotations for a given data row, if applicable -- note this does not contain required data row ID information
        }
        global_key : {
            "data_row" : {},
            "project_id" : "",
            "annotations" : []
        }
    },
    dataset_id : {
        global_key : {
            "data_row" : {},
            "project_id" : "",
            "annotations" : []
        }
        global_key : {
            "data_row" : {},
            "project_id" : "",
            "annotations" : []
        }
    }
}

This uniforms input formats so that they can leverage labelbase - Labelbox base code for best practices
"""
import pyspark
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
from labelbox import Client as labelboxClient
from labelbox.schema.ontology import Ontology as labelboxOntology
from labelspark.connector import get_table_length, get_unique_values
from labelbase.metadata import get_metadata_schema_to_name_key, process_metadata_value
from labelbase.ontology import get_ontology_schema_to_name_path
from labelbase.annotate import create_ndjsons
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

def create_upload_dict(client:labelboxClient, table:pyspark.sql.dataframe.DataFrame,
                       row_data_col:str, global_key_col:str, external_id_col:str, 
                       dataset_id_col:str, dataset_id:str,
                       project_id_col:str, project_id:str,
                       metadata_index:dict, attachment_index:dict, annotation_index:dict,
                       upload_method:str, mask_method:str, divider:str, verbose:bool, extra_client:bool=None):  
    """ Uses UDFs to create a column of data row dictionaries to-be-uploaded, then converts this column into a list
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object        
        table                       :   Required (pandas.core.frame.DataFrame) - Pandas DataFrame                
        row_data_col                :   Required (str) - Column containing asset URL or raw text
        global_key_col              :   Required (str) - Column name containing the data row global key - defaults to row data
        external_id_col             :   Required (str) - Column name containing the data row external ID - defaults to global key
        dataset_id_col              :   Required (str) - Column name containing the dataset ID to add data rows to
        dataset_id                  :   Required (str) - Labelbox dataset ID to add data rows to - only necessary if no "dataset_id" column exists            
        project_id_col              :   Required (str) - Column name containing the project ID to batch a given row to        
        project_id                  :   Required (str) - Labelbox project ID to add data rows to - only necessary if no "project_id" column exists
        metadata_index              :   Required (dict) - Dictonary where {key=metadata_field_name : value=metadata_type}
        attachment_index            :   Required (dict) - Dictonary where {key=column_name : value=attachment_type}
        annotation_index            :   Required (dict) - Dictonary where {key=column_name : value=top_level_feature_name}
        upload_method               :   Required (str) - Either "mal" or "import" - required to upload annotations (otherwise leave as "")
        mask_method                 :   Optional (str) - Specifies your input mask data format
                                            - "url" means your mask is an accessible URL (must provide color)
                                            - "array" means your mask is a numpy array (must provide color)
                                            - "png" means your mask value is a png-string                 
        divider                     :   Required (str) - String delimiter for all name keys generated
        verbose                     :   Required (bool) - If True, prints details about code execution; if False, prints minimal information
        extra_client                :   Ignore this value - necessary for other labelbase integrations                
    Returns:
        - global_key_to_upload_dict - Dictionary in the above format
    """     
    # Check that global key column is entirely unique values
    table_length = get_table_length(table=table, extra_client=extra_client)
    if verbose:
        print(f'Creating upload list - {table_length} rows in Pandas DataFrame')
    unique_global_key_count = len(get_unique_values(table=table, col=global_key_col, extra_client=extra_client))
    if table_length != unique_global_key_count:
        print(f"Warning: Your global key column is not unique - upload will resume, only uploading 1 data row per unique global key")      

    # Create your column of upload dict values using UDFs
    uploads_table = create_uploads_column(
        client=client, table=table, row_data_col=row_data_col, global_key_col=global_key_col, external_id_col=external_id_col, 
        dataset_id_col=dataset_id_col, dataset_id=dataset_id, project_id_col=project_id_col, project_id=project_id, metadata_index=metadata_index, 
        attachment_index=attachment_index, annotation_index=annotation_index, 
        upload_method=upload_method, mask_method=mask_method, divider=divider, verbose=verbose 
    )
    # Query your uploads column and create your upload dict

    upload_dict = {}
    res = uploads_table.select("uploads").rdd.map(lambda x: {'data_row': {'row_data': x['uploads']['data_row']['row_data'], 'global_key': x['uploads']['data_row']['global_key'], 'external_id': x['uploads']['data_row']['external_id'], 'metadata_fields': x['uploads']['data_row']['metadata_fields'], 'attachments': x['uploads']['data_row']['attachments']}, 'dataset_id': x['uploads']['dataset_id'], 'project_id': x['uploads']['project_id'], 'annotations': x['uploads']['annotations']}).collect()

    with ThreadPoolExecutor() as exc:
        futures = [exc.submit(process_upload, x) for x in res]
        for future in as_completed(futures):
            y = future.result()
            upload_dict[y["data_row"]["global_key"]] = {
                "data_row" : y["data_row"],
                "project_id" : y["project_id"],
                "annotations" : y["annotations"],
                "dataset_id" : y["dataset_id"]
            }        
    return upload_dict

def create_uploads_column(client:labelboxClient, table:pyspark.sql.dataframe.DataFrame,
                          row_data_col:str, global_key_col:str, external_id_col:str, 
                          dataset_id_col:str, dataset_id:str, project_id_col:str, project_id:str,
                          metadata_index:dict, attachment_index:dict, annotation_index:dict,
                          upload_method:str, mask_method:str, divider:str, verbose:bool, extra_client:bool=None):
    """ Takes a table and returns a new table with an "uploads" column where the values are
    {
        "data_row" : {
            "global_key" : "",                    |
            "row_data" : "",                      |
            "external_id" : "",                   | ---- This is your data row upload as a dictionary
            "metadata_fields" : [],               |
            "attachments" : []                    |
        }, 
        "dataset_id" : "" -- This is the dataset ID to upload this data row to
        "project_id" : "", -- This batches data rows to projects, if applicable
        "annotations" : [] -- List of annotations for a given data row, if applicable (note that data row ID is not included)
    }
    """
    # Get dictionary where {key=project_id : value=ontology_index} -- index created by labelbase -- if project IDs are available
    if project_id != "":
        project_ids = [project_id]
    elif project_id_col != "":
        project_ids = get_unique_values(table=table, col=project_id_col, extra_client=extra_client)
    else:
        project_ids = []
    project_id_to_ontology_index = {}        
    if (project_ids!=[]) and (annotation_index!={}) and (upload_method in ["mal", "import"]): # If we're uploading annotations
        for projectId in project_ids:
            ontology = client.get_project(projectId).ontology()
            project_type = client.get_project(projectId).media_type
            project_id_to_ontology_index[projectId] = get_ontology_schema_to_name_path(ontology=ontology,divider=divider,invert=True,detailed=True)
            project_id_to_ontology_index[projectId]['project_type'] = str(project_type)

    project_id_to_ontology_index_bytes = json.dumps(project_id_to_ontology_index)          
    # Create your upload column's syntax
    upload_schema = StructType([
        StructField(
            "data_row", StructType([
                StructField("row_data", StringType()), StructField("global_key", StringType()), StructField("external_id", StringType()),
                StructField("metadata_fields", ArrayType(MapType(StringType(), StringType(), True))),
                StructField("attachments", ArrayType(MapType(StringType(), StringType(), True)))
            ])
        ),
        StructField("dataset_id", StringType()), StructField("project_id", StringType()), 
        StructField("annotations", ArrayType(MapType(StringType(), StringType(), True)))
    ])
    x = get_metadata_schema_to_name_key(client=client, divider=divider, invert=True) # Get metadata dict where {key=name : value=schema_id}
    metadata_name_key_to_schema_bytes = json.dumps(x) # Convert reference dict to bytes    
    # Run a UDF to create row values
    data_rows_udf = udf(create_data_row, upload_schema)    
    project_input = lit(project_id_col) if not project_id_col else project_id_col
    dataset_input = lit(dataset_id_col) if not dataset_id_col else dataset_id_col

    table = table.withColumn(
      'uploads', data_rows_udf(
          row_data_col, global_key_col, external_id_col, lit(metadata_name_key_to_schema_bytes),
          lit(project_id_col), project_input, lit(project_id), lit(dataset_id_col), dataset_input, lit(dataset_id)
      )
    )
    print(table.collect()[0])
    # Run a UDF to add attachments, if applicable  
    if attachment_index:
        print(attachment_index)
        attachments_udf = udf(create_attachments, upload_schema)  # Create a UDF
        for attachment_column_name in attachment_index: # Run this UDF for each attachment column in the attachment index
            attachment_type = attachment_index[attachment_column_name]
            table = table.withColumn('uploads', attachments_udf('uploads', lit(attachment_type), attachment_column_name))        
    # Run a UDF to add metadata, if applicable
    if metadata_index:
        print(metadata_index)
        metadata_udf = udf(create_metadata, upload_schema) # Create a UDF
        for metadata_field_name in metadata_index: # Run this UDF for each metadata field name in the metadata index
            metadata_type = metadata_index[metadata_field_name]
            metadata_column_name = f"metadata{divider}{metadata_type}{divider}{metadata_field_name}"
            table = table.withColumn(
                'uploads', metadata_udf(
                    'uploads', lit(metadata_field_name), metadata_column_name, lit(metadata_type), lit(metadata_name_key_to_schema_bytes), lit(divider)
                )
            )    
    # Run a UDF to add annotations, if applicable 
    if (annotation_index!={}) and (project_id_to_ontology_index!={}) and (upload_method in ["mal", "import"]):
        annotation_udf = udf(create_annotations, upload_schema) # Create a UDF
        for annotation_column_name in annotation_index: # Run this UDF for each attachment column name in the attachment index
            top_level_feature_name = annotation_index[annotation_column_name]
            annotation_type = annotation_column_name.split(divider)[1]
            table = table.withColumn(
              'uploads', annotation_udf(
                  'uploads', lit(top_level_feature_name), annotation_column_name, lit(mask_method), lit(annotation_type), lit(project_id_to_ontology_index_bytes), lit(divider)
              )
            )        
    return table

def create_data_row(row_data_col, global_key_col, external_id_col, metadata_name_key_to_schema_bytes,
                    project_id_col_name, project_id_col_value, project_id_str,
                    dataset_id_col_name, dataset_id_col_value, dataset_id_str):
    """ Function to-be-wrapped in a UDF that creates upload dict values (without metadata, attachments or annotations)
    """
    metadata_name_key_to_schema = json.loads(metadata_name_key_to_schema_bytes)
    if project_id_col_name:
        projectId = project_id_col_value
    elif project_id_str:
        projectId = project_id_str
    else:
        projectId = ""    
    if dataset_id_col_name:
        datasetId = dataset_id_col_value
    else:
        datasetId = dataset_id_str        
    data_row_dict = {
        "row_data" : row_data_col, "external_id" : external_id_col, "global_key" : global_key_col,
        "metadata_fields" : [{"schema_id" : metadata_name_key_to_schema["lb_integration_source"], "value" : "Databricks"}],
        "attachments" : []
    }
    return { "data_row" : data_row_dict, "project_id" : projectId, "dataset_id" : datasetId, "annotations" : [] }

def create_metadata(uploads_col, metadata_field_name, metadata_col_value, metadata_type, metadata_name_key_to_schema_bytes, divider):
    """ Function to-be-wrapped in a UDF that adds metadata fields to your upload dict
    """  
    metadata_name_key_to_schema = json.loads(metadata_name_key_to_schema_bytes)
    input_metadata = process_metadata_value(
        metadata_value=metadata_col_value, metadata_type=metadata_type, parent_name=metadata_field_name, 
        metadata_name_key_to_schema=metadata_name_key_to_schema, divider=divider
    )            
    if input_metadata:
        uploads_col["data_row"]["metadata_fields"].append({"schema_id" : metadata_name_key_to_schema[metadata_field_name], "value" : input_metadata})
    return uploads_col

def create_attachments(uploads_col, attachment_type, attachment_col_value):
    """ Function to-be-wrapped in a UDF that adds attachments to your upload dict
    """  
    if attachment_col_value:
        uploads_col["data_row"]["attachments"].append({"type" : attachment_type, "value" : attachment_col_value})
    return uploads_col  

def create_annotations(uploads_col, top_level_feature_name, annotations, mask_method, annotation_type, project_id_to_ontology_index_bytes, divider):
    """ Function to-be-wrapped in a UDF that adds attachments to your upload dict
    """  
    project_id_to_ontology_index = json.loads(project_id_to_ontology_index_bytes)
    ontology_index = project_id_to_ontology_index[uploads_col["project_id"]]
    print(annotations)
    if annotations is not None:
        annotation_list = []
        ndjsons = create_ndjsons(
                    top_level_name=top_level_feature_name,
                    annotation_inputs=annotations,
                    ontology_index=ontology_index,
                    mask_method=mask_method,
                    divider=divider    
                )
        for ndjson in ndjsons:
            annotation_list.append({annotation_type:json.dumps(ndjson)})
        print(annotation_list)
        uploads_col["annotations"].extend(
            annotation_list
        )
    return uploads_col


def process_upload(x):
    """ Function to-be-multithreaded that processes rows into the upload_dict format
    """
    annotations = [string_to_ndjson(x) for x in x["annotations"]]
    return {
        "data_row" : x["data_row"],
        "project_id" : x["project_id"],
        "annotations" : annotations,
        "dataset_id" : x["dataset_id"]
    }

def string_to_ndjson(annotation):
    """ Function that takes stringtype representation of an annotation and converts it into the Labelbox ndjson format
    """
    for key, value in annotation.items():
        annotation[key] = json.loads(value)
    return annotation
    
