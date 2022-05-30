from pyspark.sql.functions import col
from labelspark.constants import LABELBOX_DEFAULT_TYPE_DICTIONARY

def dataframe_schema_enrichment(raw_dataframe, type_dictionary=None):
    if type_dictionary is None:
        type_dictionary = LABELBOX_DEFAULT_TYPE_DICTIONARY
    copy_dataframe = raw_dataframe
    for column_name in type_dictionary:
        try:
            copy_dataframe = copy_dataframe.withColumn(
                column_name,
                col(column_name).cast(type_dictionary[column_name]))
        except Exception as e:
            print(e.__class__, "occurred for", column_name, ":",
                  type_dictionary[column_name], ". Please check that column.")

    return copy_dataframe