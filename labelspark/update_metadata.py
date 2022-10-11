from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType
import json

def update_metadata(client, spark_dataframe, metadata_field_name, lb_dataset):
    """ Updates a spark dataframe with the current Labelbox metadata value given a metadata field name
    Args:
        client                      :       labelbox.Client object
        spark_dataframe             :       pyspark.sql.dataframe.Dataframe object - must have "data_row_id" and metadata_field_name columns at a minimum
        metadata_field_name         :       Name of the column in the spark_dataframe to-be-updated. This must also be the name of the metadata field in Labelbox
        lb_dataset                  :       labelbox.schema.dataset.Dataset object that hosts the data row IDs in question
    Returns:
        Updated spark_dataframe
    """

    mdo = client.get_data_row_metadata_ontology()

    data_row_ids = [data_row.uid for data_row in lb_dataset.export_data_rows()]
    data_row_metadata = mdo.bulk_export(data_row_ids)

    mdo_reserved_by = mdo.reserved_by_name
    mdo_reserved_by.update(mdo.custom_by_name)
    metadata_field_schema_id = mdo_reserved_by[metadata_field_name].uid

    update_dict = {}
    for data_row in data_row_metadata:
        key = data_row.data_row_id
        for field in data_row.fields:
            if field.schema_id == metadata_field_schema_id:
                value = field.value
                update_dict[key] = value  

    def sync_function(update_dict, data_row_id, current_metadata_value):
        """ Nested UDF Functionality update metadata in Labelbox and in the pyspark dataframe
        Args:  
            update_dict                 :      Dictionary where {key=data_row_id and value=latest_metadata_value}
            data_row_id                 :      Labelbox Data Row ID
            current_metadata_value      :      The existing value for this metadata field in the spark_dataframe
        Returns:
            New value to-be-inserted in the column corresponding to this metadata field
        """  
        reference_dict = json.loads(update_dict)
        if data_row_id in reference_dict.keys():
            return_value = reference_dict[data_row_id]
        else:
            return_value = current_metadata_value
        return return_value                
                
    sync_udf = udf(sync_function, StringType())

    return spark_dataframe.withColumn(metadata_field_name, sync_udf(lit(json.dumps(update_dict)), "data_row_id", metadata_field_name))
