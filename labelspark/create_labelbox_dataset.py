from labelbox.schema.data_row_metadata import DataRowMetadataKind as lb_metadata_type
from pyspark import SparkContext
from packaging import version
import datetime
import json
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
from pyspark.sql.functions import udf, lit

def create_labelbox_dataset(client, spark_dataframe, dataset_name=str(datetime.datetime.now()), add_data_row_ids=True, iam_integration='DEFAULT', metadata_index=False, **kwargs):
  """ Creates a Labelbox dataset and creates data rows given a spark dataframe. Uploads data rows in batches of 10,000.
  Args:
      client                  :     labelbox.Client object
      spark_dataframe         :     pyspark.sql.dataframe.Dataframe object - must have "row_data" and "external_id" columns at a minimum
      dataset_name            :     Labelbox dataset name      
      iam_integration         :     IAM integreation to use when creating the Labelbox dataset
      add_data_row_ids        :     Boolean - if True, adds a "data_row_id" column - if False, does nothing
      metadata_index          :     (Optional) Dictionary to add metadata to your data rows. Synatx is {key=column_name : value=metadata_type}
                                          Metadata type is one of the following strings:
                                              "enum"
                                              "string"
                                              "number"
                                              "datetime"
  Returns:
    spark_dataframe           :     updated spark_dataframe
    lb_dataset                :     labelbox.schema.dataset.Dataset object
  """
  lb_dataset = client.create_dataset(name=dataset_name, iam_integration=iam_integration, **kwargs)
  connect_spark_metadata(client, spark_dataframe, metadata_index) 
  print("Dataset created in Labelbox.")
  # Creates a column in dictionary format to-be-uploaded to Labelbox
  spark_dataframe = create_uploads_column(spark_dataframe, client, metadata_index)
  # Batch uploads data rows
  pandas_df = spark_dataframe.to_pandas_on_spark()
  print(f'Uploading {len(pandas_df)} to Labelbox in dataset with ID {lb_dataset.uid}')
  lb_dataset = batch_upload_data_rows(lb_dataset, pandas_df, 10000)
  # Adds a data_row_id column to your dataframe
  if add_data_row_ids:
    print(f'Attaching Laeblbox Data Row IDs to Spark Table')
    spark_dataframe = attach_data_row_ids(lb_dataset, spark_dataframe)
  return lb_dataset, spark_dataframe

### Connect Spark Metadata
def connect_spark_metadata(client, spark_dataframe, metadata_index):
  """ Checks to make sure all desired metadata for upload has a corresponding field in Labelbox. Note limits on metadata field options, here https://docs.labelbox.com/docs/limits
  Args:  
    client                    :    labelbox.Client object
    spark_dataframe           :    pyspark.sql.dataframe.Dataframe object
    metadata_index            :    Dictionary to add metadata to your data rows. Synatx is {key=column_name : value=either "enum", "string", "number" or "datetime"}
  Returns:
    Nothing - the metadata ontology has been updated
  """
  conversion = {
    "enum" : lb_metadata_type.enum,
    "string" : lb_metadata_type.string,
    "datetime" : lb_metadata_type.datetime,
    "number" : lb_metadata_type.number
  } 
  if metadata_index:
    print("Connecting Metadata to Labelbox")
    lb_metadata_index = {}      
    mdo = client.get_data_row_metadata_ontology()
    mdo_dict = mdo._get_ontology()
    labelbox_metadata_names = [field['name'] for field in mdo_dict]
    for column_name in metadata_index.keys():
      lb_metadata_index.update({column_name : conversion[metadata_index[column_name]]})  
    for spark_metadata_name in list(metadata_index.keys()):
      if spark_metadata_name not in labelbox_metadata_names:
        metadata_type = lb_metadata_index[spark_metadata_name]
        create_metadata_field(mdo, spark_dataframe, spark_metadata_name, metadata_type)
  if "lb_integration_source" not in labelbox_metadata_names:
      metadata_type = lb_metadata_type.string
      create_metadata_field(mdo, spark_dataframe, "lb_integration_source", metadata_type)
  return lb_metadata_index

### Connect Spark Metadata
def create_metadata_field(metadata_ontology_object, spark_dataframe, spark_metadata_name, metadata_type):
  """ Given a metadata field name and a column, creates a metadata field in Laeblbox given a labelbox metadata type
  Args:
    metadata_ontology_object  :    labelbox.schema.data_row_metadata.DataRowMetadataOntology object
    spark_dataframe           :    pyspark.sql.dataframe.Dataframe object
    spark_metadata_name       :    Name of the column in the pyspark table to-be-converted into a metadata field
    labelbox_metadata_type    :    labelbox.schema.data_row_metadata.DataRowMetadataKind object
  Returns:
    Nothing - the metadata ontology now has a new field can can be refreshed
  """
  if metadata_type == lb_metadata_type.enum:
    enum_options = [str(x.__getitem__(spark_metadata_name)) for x in spark_dataframe.select(spark_metadata_name).distinct().collect()]
  else:
    enum_options = None
  metadata_ontology_object.create_schema(name=spark_metadata_name, kind=metadata_type, options=enum_options) 
  
## Create Uploads Column
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
    # Catches Enum Metadata
    if type(metadata_dict[name]) == dict:
      for enum_option in metadata_dict[name]:
        mdo_lookup.update({
          str(enum_option) : {
            "feature_schema_id" : metadata_dict[name][enum_option].uid, 
            "parent" : metadata_dict[name][enum_option].parent
          }
        })
    else:
      mdo_lookup.update({
        name : {
          "feature_schema_id" : metadata_dict[name].uid
        }
      })
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

## Create Uploads Column
def create_uploads(row_data, external_id, mdo_lookup_bytes):
  """ Function to-be-wrapped into a user-defined function
  Args:
      row_data                :     row_data value
      external_id             :     external_id value      
      mdo_lookup_bytes        :     Bytearray representation of a dictionary where {key=metadata_name : value=metadata_feature_schema_id or value={"parent", "feature_schema_id"}} for enum metadata    
  Returns:
      Data row upload as-a-dictionary
  """
  mdo_lookup = json.loads(mdo_lookup_bytes)
  return {
    "row_data" : row_data,
    "external_id" : external_id,
    "metadata_fields" : [
      {
        "schema_id" : mdo_lookup["lb_integration_source"]['feature_schema_id'],
        "value" : "Databricks"
      }
    ]
  }

## Create Uploads Column
def attach_metadata(metadata_value, data_row, column_name, mdo_lookup_bytes, metadata_index_bytes):
  """ Function to-be-wrapped into a user-defined function
  Args:
      metadata_value          :     Value for the metadata field
      data_row                :     Data row dictionary to add metadata fields to
      column_name             :     Name of the column holding the metadata value
      mdo_lookup_bytes        :     Bytearray representation of a dictionary where {key=metadata_name : value=metadata_feature_schema_id or value={"parent", "feature_schema_id"}} for enum metadata    
      metadata_index_bytes    :     Bytearray representation of a dictionary where {key=metadata_name : value=metadata_type as string}
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
  elif (metadata_value is not None) or (str(metadata_value) != ""):
    data_row['metadata_fields'].append({
      "schema_id" : mdo_lookup[column_name]['feature_schema_id'],
      "value" : str(metadata_value)
    })
  return data_row  

# Batch Upload Data Rows
def batch_upload_data_rows(lb_dataset, pandas_df, upload_batch_size):
  """ Batch Uploads Data Row IDs given a list of uploads and a batch size
  Args: 
    pandas_df                 :     pyspark.pandas.frame.DataFrame version of the working pyspark dataframe
    upload_batch_size         :     Integer - Maximum is 30,000 if there's metadata on data rows
  Returns:
    updated Labelbox dataset DB Object
  """
  starttime = datetime.datetime.now()
  for i in range(0, len(pandas_df), upload_batch_size):
    if i+upload_batch_size<=len(pandas_df):
      batch_df = pandas_df.iloc[i:i+upload_batch_size]
    else:
      batch_df = pandas_df.iloc[i:]
    print(f'Batch Number {int(1+(i/upload_batch_size))} with {len(batch_df)} data rows')
    batch_spark_df = batch_df.to_spark()
    batch_upload = batch_spark_df.select("uploads").rdd.map(lambda x: x.uploads.asDict()).collect()
    task = lb_dataset.create_data_rows(batch_upload)
    task.wait_till_done() 
    print(f'Upload Speed: {datetime.datetime.now()-starttime}')
    print(f'Errors: {task.errors}')
    starttime = datetime.datetime.now()    
  return lb_dataset

# Attach data row IDs
def attach_data_row_ids(lb_dataset, spark_dataframe):
  """ UDF to add data row IDs to a dataframe given external_id and relevant labelbox dataset. Assumption is that external_id is unique
  Args:  
    spark_dataframe           :     pyspark.sql.dataframe.Dataframe object - must have "external_id" column
    lb_dataset                :     labelbox.schema.dataset.Dataset object
  Returns:
    pyspark dataframe with a completed data_row_id column filled in
  """
  external_id_to_data_row_id = {}
  data_rows = lb_dataset.export_data_rows()
  for data_row in data_rows:
    external_id_to_data_row_id.update({str(data_row.external_id) : str(data_row.uid)})
  find_data_row_id_udf = udf(find_data_row_id, StringType())
  return spark_dataframe.withColumn('data_row_id', find_data_row_id_udf("external_id", lit(json.dumps(external_id_to_data_row_id))))

# Attach data row IDs
def find_data_row_id(external_id, reference):
  """ Nested UDF Functionality to return a data row ID given the external ID
  Args:
    external_id               :      Data Row external ID as a string
    reference                 :      dictionary where {key=external_id : value=data_row_id}
  Returns:
    Data Row ID value
  """
  reference_dict = json.loads(reference)
  data_row_id = reference_dict[external_id]
  return data_row_id
