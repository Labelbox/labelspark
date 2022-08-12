from labelbox.schema.data_row_metadata import DataRowMetadataKind as lb_metadata_type
from pyspark import SparkContext
from packaging import version
from datetime import datetime

import json
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
from pyspark.sql.functions import udf, lit

def create_dataset(client, spark_dataframe, dataset_name=str(datetime.now()), iam_integration='DEFAULT', metadata_index=False, **kwargs):
  """ Creates a Labelbox dataset and creates data rows given a spark dataframe. Uploads data rows in batches of 10,000.
  Args:
      client                  :     labelbox.Client object
      spark_dataframe         :     pyspark.sql.dataframe.Dataframe object - must have "row_data" and "external_id" columns at a minimum
      dataset_name            :     Labelbox dataset name      
      iam_integration (str)   :     IAM integreation to use when creating the Labelbox dataset
      metadata_index          :     (Optional) Dictionary to add metadata to your data rows. Synatx is {key=column_name : value=metadata_type}
                                          Metadata type is one of the following strings:
                                              "enum"
                                              "string"
                                              "number"
                                              "datetime"
  """
  lb_dataset = client.create_dataset(name=dataset_name, iam_integration=iam_integration, **kwargs)

  if metadata_index:
    conversion = {
      "enum" : lb_metadata_type.enum,
      "string" : lb_metadata_type.string,
      "datetime" : lb_metadata_type.datetime,
      "number" : lb_metadata_type.number
    } 
    lb_metadata_index = {}
    for column_name in metadata_index.keys():
      lb_metadata_index.update({column_name : conversion[metadata_index[column_name]]})
    print("Connecting Metadata to Labelbox")
  else:
    lb_metadata_index = False
  
  # Connects Metadata
  connect_spark_metadata(client, spark_dataframe, lb_metadata_index)    
  
  print("Dataset created in Labelbox.")
  
  uploads_spark_dataframe = create_uploads_column(spark_dataframe, client, metadata_index)
  
  pandas_df = uploads_spark_dataframe.to_pandas_on_spark()
  
  upload_batch_size = 10000
  
  starttime = datetime.now()
  
  print(f'Uploading {len(pandas_df)} to Labelbox in dataset with ID {lb_dataset.uid}')
  
  for i in range(0, len(pandas_df), upload_batch_size):
    if i+upload_batch_size<=len(pandas_df):
      batch_df = pandas_df.iloc[i:i+upload_batch_size]
    else:
      batch_df = pandas_df.iloc[i:]
    print(f'Batch Number {int(1+(i/upload_batch_size))} with {len(batch_df)} data rows')
    batch_upload = create_data_row_uploads(batch_df.to_spark())
    task = lb_dataset.create_data_rows(batch_upload)
    task.wait_till_done() 
    print(f'Upload Time: {datetime.now()-starttime}')
    starttime = datetime.now()
  
  return lb_dataset

def create_uploads_column(spark_dataframe, client, metadata_index=False):
  """ Creates a colum using the pyspark StructType class that consists of row_data, external_id, and metadata_fields if the `metadata_index` argument is provided
  Args:
      client                  :     labelbox.Client object
      spark_dataframe         :     pyspark.sql.dataframe.Dataframe object - must have "row_data" and "external_id" columns at a minimum 
      metadata_index          :     (Optional) Dictionary where {key=column_name : value=metadata_type} (where metadata_type is "enum", "number", "string" or "datetime"). If not provided, will not add metadata to the data row
  Returns:
      spark_dataframe with an `uploads` column that can be converted to a Labelbox data row upload dictionary
  """
  # Grab the metadata ontology and create a dictionary where {key=metadata_name : value=metadata_feature_schema_id} or {key=metadata_name : value={"parent", "feature_schema_id"}} for enum metadata
  mdo = client.get_data_row_metadata_ontology()
  metadata_dict = mdo.reserved_by_name
  metadata_dict.update(mdo.custom_by_name)
  mdo_lookup = {}
  for name in metadata_dict:
    if type(metadata_dict[name]) == dict:
      for enum_option in metadata_dict[name]:
        fsid = metadata_dict[name][enum_option].uid
        parent = metadata_dict[name][enum_option].parent
        mdo_lookup.update({str(enum_option) : {"feature_schema_id" : fsid, "parent" : parent}})
    else:
      fsid = metadata_dict[name].uid
      mdo_lookup.update({name : fsid})
  
  # Specify the structure of the `uploads` column
  upload_schema = StructType([
    StructField("row_data", StringType()),
    StructField("external_id", StringType()),
    StructField("metadata_fields", ArrayType(MapType(StringType(), StringType(), True)))
  ])
  
  # Create an `uploads` column with row_data and external_id
  create_uploads_udf = udf(create_uploads, upload_schema)
  df = spark_dataframe.withColumn('uploads', create_uploads_udf('row_data', 'external_id', lit(json.dumps(mdo_lookup))))
  
  # Attach metadata to the `uploads` column if metadata_index argument is provided
  if metadata_index:
    attach_metadata_udf = udf(attach_metadata, upload_schema)
    for column_name in metadata_index:
      df = df.withColumn('uploads', attach_metadata_udf(column_name, 'uploads', lit(column_name), lit(json.dumps(mdo_lookup)), lit(json.dumps(metadata_index))))
    
  return df

def create_uploads(row_data, external_id, mdo_lookup_bytes):
  """ Function to-be-wrapped into a user-defined function
  Args:
      row_data                :     row_data value
      external_id             :     external_id value      
      mdo_lookup_bytes        :     Bytearray representation of a dictionary where {key=column_name : value=metadata_type} (where metadata_type is "enum", "number", "string" or "datetime"). If not provided, will not add metadata to the data row
  Returns:
      Data row upload as-a-dictionary
  """
  mdo_lookup = json.loads(mdo_lookup_bytes)
  return {
    "row_data" : row_data,
    "external_id" : external_id,
    "metadata_fields" : [
      {
        "schema_id" : mdo_lookup["lb_integration_source"],
        "value" : "Databricks"
      }
    ]
  }

def attach_metadata(metadata_value, data_row, column_name, mdo_lookup_bytes, metadata_index_bytes):
  """ Function to-be-wrapped into a user-defined function
  Args:
      metadata_value          :     Value for the metadata field
      data_row                :     Data row dictionary to add metadata fields to
      column_name             :     Name of the column holding the metadata value
      mdo_lookup_bytes        :     Bytearray representation of a dictionary where {key=metadata_name : value=metadata_feature_schema_id} or {key=metadata_name : value={"parent", "feature_schema_id"}} for enum metadata    
      mdo_lookup_bytes        :     Bytearray representation of a dictionary where {key=metadata_name : value=metadata_feature_schema_id} or {key=metadata_name : value={"parent", "feature_schema_id"}} for enum metadata          
  Returns:
      Data row upload as-a-dictionary
  """  
  mdo_lookup = json.loads(mdo_lookup_bytes)
  metadata_type = json.loads(metadata_index_bytes)[column_name]
  if (metadata_type == "enum") and (metadata_value is not None):
    data_row['metadata_fields'].append({
      "schema_id" : mdo_lookup[str(metadata_value)]['parent'],
      "value" : mdo_lookup[str(metadata_value)]['feature_schema_id']     
    })
  elif metadata_value is not None:
    data_row['metadata_fields'].append({
      "schema_id" : mdo_lookup[column_name],
      "value" : str(metadata_value)
    })
  return data_row

def create_data_row_uploads(spark_dataframe):
  """ Function to-be-wrapped into a user-defined function
  Args:
      metadata_value          :     Value for the metadata field
      data_row                :     Data row dictionary to add metadata fields to
      column_name             :     Name of the column holding the metadata values
      mdo_lookup_bytes        :     Bytearray representation of a dictionary where {key=metadata_name : value=metadata_feature_schema_id} or {key=metadata_name : value={"parent", "feature_schema_id"}} for enum metadata    
      mdo_lookup_bytes        :     Bytearray representation of a dictionary where {key=metadata_name : value=metadata_feature_schema_id} or {key=metadata_name : value={"parent", "feature_schema_id"}} for enum metadata          
  Returns:
      List of data row upload dictionaries as-a-dictionary
  """  
  def structure_data_row(pyspark_row):
    """ Function to take pyspark StructType column and convert into an uploadable data row dictionary
    Args:
        pyspark_row             :     Row object from a pyspark dataframe
    Returns:
        Dictionary with "row_data", "external_id" and "metadata_fields" keys to-be-uploaded to Labelbox
    """
    return {
      "row_data" : pyspark_row.uploads.row_data,
      "external_id" : pyspark_row.uploads.external_id,
      "metadata_fields" : pyspark_row.uploads.metadata_fields
    }
  
  upload_list = spark_dataframe.select("uploads").rdd.map(lambda x: x.uploads.asDict()).collect()
  
  return upload_list

def connect_spark_metadata(client, spark_dataframe, lb_metadata_index):
  """ Checks to make sure all desired metadata for upload has a corresponding field in Labelbox. Note limits on metadata field options, here https://docs.labelbox.com/docs/limits
  Args:
    client                          :    labelbox.Client object
    spark_dataframe                 :    pyspark.sql.dataframe.Dataframe object
    labelbox_metadata_type_index    :    Dictionary where {key=column_name : value=labelbox.schema.data_row_metadata.DataRowMetadataKind options}
  Returns:
    Nothing - the metadata ontology has been updated
  """
  mdo = client.get_data_row_metadata_ontology() #query Labelbox for metadata fields 
  labelbox_metadata_names = [field['name'] for field in mdo._get_ontology()] #produce list of all metadata names in ontology
  
  if lb_metadata_index:
    spark_metadata_names = list(lb_metadata_index.keys())
    for spark_metadata_name in spark_metadata_names:
#       mdo = client.get_data_row_metadata_ontology()
#       labelbox_metadata_names = [field['name'] for field in mdo._get_ontology()]
      if spark_metadata_name not in labelbox_metadata_names:
        metadata_type = lb_metadata_index[spark_metadata_name]
        create_metadata_field(mdo, spark_dataframe, spark_metadata_name, metadata_type)
  if "lb_integration_source" not in labelbox_metadata_names:
      metadata_type = lb_metadata_type.string
      create_metadata_field(mdo, spark_dataframe, "lb_integration_source", metadata_type)

def create_metadata_field(metadata_ontology_object, spark_dataframe, spark_metadata_name, metadata_type):
  """ Given a metadata field name and a column, creates a metadata field in Laeblbox given a labelbox metadata type
  Args:
    metadata_ontology_object        :    labelbox.schema.data_row_metadata.DataRowMetadataOntology object
    spark_dataframe                 :    pyspark.sql.dataframe.Dataframe object
    spark_metadata_name             :    Name of the column in the pyspark table to-be-converted into a metadata field
    labelbox_metadata_type          :    labelbox.schema.data_row_metadata.DataRowMetadataKind object
  Returns:
    Nothing - the metadata ontology now has a new field can can be refreshed
  """
  if metadata_type == lb_metadata_type.enum:
    enum_options = [str(x.__getitem__(spark_metadata_name)) for x in spark_dataframe.select(spark_metadata_name).distinct().collect()]
  else:
    enum_options = None
  metadata_ontology_object.create_schema(name=spark_metadata_name, kind=metadata_type, options=enum_options)  
