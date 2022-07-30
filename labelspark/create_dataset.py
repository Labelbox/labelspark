from labelbox.schema.data_row_metadata import DataRowMetadataKind as metadata_type
from pyspark import SparkContext
from packaging import version
from datetime import datetime
sc = SparkContext.getOrCreate()
if version.parse(sc.version) < version.parse("3.2.0"):
  import databricks.koalas as pd
  needs_koalas = True
else:
  import pyspark.pandas as pd
  needs_koalas = False  

def create_dataset(client, spark_dataframe, dataset_name=str(datetime.now()), iam_integration='DEFAULT', metadata_index=False, **kwargs):
  """ Creates a Labelbox dataset and creates data rows given a spark dataframe
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
      "enum" : metadata_type.enum,
      "string" : metadata_type.string,
      "datetime" : metadata_type.datetime,
      "number" : metadata_type.number
    } 
    labelbox_metadata_type_index = {}
    for column_name in metadata_index.keys():
      labelbox_metadata_type_index.update({column_name : conversion[metadata_index[column_name]]})
  else:
    labelbox_metadata_type_index = False

  connect_spark_metadata(client, spark_dataframe, labelbox_metadata_type_index)
  data_row_upload = create_spark_data_rows(client, spark_dataframe, labelbox_metadata_type_index)
  upload_task = lb_dataset.create_data_rows(data_row_upload)
  upload_task.wait_till_done()
  print("Dataset created in Labelbox.")
  return lb_dataset

def connect_spark_metadata(client, spark_dataframe, labelbox_metadata_type_index):
  """ Checks to make sure all desired metadata for upload has a corresponding field in Labelbox. Note limits on metadata field options, here https://docs.labelbox.com/docs/limits
  Args:
    client                          :    labelbox.Client object
    spark_dataframe                 :    pyspark.sql.dataframe.Dataframe object
    labelbox_metadata_type_index    :    Dictionary where {key=column_name : value=labelbox.schema.data_row_metadata.DataRowMetadataKind options}
  Returns:
    Nothing - the metadata ontology has been updated
  """
  mdo = client.get_data_row_metadata_ontology()
  mdo_dict = mdo._get_ontology()
  labelbox_metadata_names = [field['name'] for field in mdo_dict]
  if labelbox_metadata_type_index:
    spark_metadata_names = list(labelbox_metadata_type_index.keys())
    for spark_metadata_name in spark_metadata_names:
      if spark_metadata_name not in labelbox_metadata_names:
        ## If the metadata field isn't present in Labelbox, create it and refresh the object
        labelbox_metadata_type = labelbox_metadata_type_index[spark_metadata_name]
        create_metadata_field(mdo, spark_dataframe, spark_metadata_name, labelbox_metadata_type)
        mdo.refresh_ontology()
        mdo_dict = mdo._get_ontology()
        labelbox_metadata_names = [field['name'] for field in mdo_dict]
  if "lb_integration_source" not in labelbox_metadata_names:
      labelbox_metadata_type = metadata_type.string
      create_metadata_field(mdo, spark_dataframe, "lb_integration_source", labelbox_metadata_type)

def create_metadata_field(metadata_ontology_object, spark_dataframe, spark_metadata_name, labelbox_metadata_type):
  """ Given a metadata field name and a column, creates a metadata field in Laeblbox given a labelbox metadata type
  Args:
    metadata_ontology_object        :    labelbox.schema.data_row_metadata.DataRowMetadataOntology object
    spark_dataframe                 :    pyspark.sql.dataframe.Dataframe object
    spark_metadata_name             :    Name of the column in the pyspark table to-be-converted into a metadata field
    labelbox_metadata_type          :    labelbox.schema.data_row_metadata.DataRowMetadataKind object
  Returns:
    Nothing - the metadata ontology now has a new field can can be refreshed
  """
  from labelbox.schema.data_row_metadata import DataRowMetadataKind as metadata_type
  if labelbox_metadata_type == metadata_type.enum:
    enum_options = [str(x.__getitem__(spark_metadata_name)) for x in spark_dataframe.select(spark_metadata_name).distinct().collect()]
  else:
    enum_options = None
  metadata_ontology_object.create_schema(name=spark_metadata_name, kind=labelbox_metadata_type, options=enum_options)  

def create_spark_data_rows(client, spark_dataframe, labelbox_metadata_type_index):
  """ Creates data rows given a spark table and an index that assigns a metadata type to a column name
  Args:
    client                          :    labelbox.Client object
    spark_dataframe                 :    pyspark.sql.dataframe.Dataframe object
    labelbox_metadata_type_index    :    Dictionary where key = column name and value = one of the enum options from labelbox.schema.data_row_metadata.DataRowMetadataKind class
  Returns:
    List of data row dictionaries to-be-uploaded
  """
  data_rows_upload = []
  mdo = client.get_data_row_metadata_ontology()
  metadata_dict = mdo.reserved_by_name
  metadata_dict.update(mdo.custom_by_name)
  for row in spark_dataframe.collect():
    data_row_dict = {
      "row_data" : row.__getitem__("row_data"),
      "external_id" : row.__getitem__("external_id")
    }
    data_row_dict['metadata_fields'] = []
    if labelbox_metadata_type_index:
      for medata_field in labelbox_metadata_type_index:
        if labelbox_metadata_type_index[medata_field] == metadata_type.enum:
          enum_value = metadata_dict[medata_field][str(row.__getitem__(medata_field))]
          data_row_dict['metadata_fields'].append({
            "schema_id" : enum_value.parent,
            "value" : enum_value.uid        
          })
        elif row.__getitem__(medata_field) is not None:
          data_row_dict['metadata_fields'].append({
            "schema_id" : metadata_dict[medata_field].uid,
            "value" : str(row.__getitem__(medata_field)) 
          })
    data_row_dict['metadata_fields'].append({
      "schema_id" : metadata_dict["lb_integration_source"].uid,
      "value" : "Databricks"
    })
    data_rows_upload.append(data_row_dict)
  return data_rows_upload
