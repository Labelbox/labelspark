from pyspark.sql.functions import col
from labelspark.spark_schema_to_string import spark_schema_to_string

# processes the bronze table into a flattened one
def flatten_bronze_table(bronze_table):
    schema_fields_array = spark_schema_to_string(
        bronze_table.schema.jsonValue())  #generator of column names
    # Note that you cannot easily access some nested fields if you must navigate arrays of arrays to get there,
    # so I do try/except to avoid parsing into those fields.
    # I believe this can be enriched with some JSON parsing, but maybe another day.
    valid_schemas = []
    for schema_field in schema_fields_array:
        success = None
        try:
            success = bronze_table.select(col(schema_field))
            if success is not None:
                valid_schemas.append(schema_field)
        except Exception as e:
            # print(e.__class__, "occurred for", schema_field, "as it is probably inside an array of JSONs")
            schema_field_up_one_level = ".".join(
                schema_field.split(".")[:-1]
            )  # very complicated way of popping the last hierarchy level
            try:
                bronze_table.select(col(schema_field_up_one_level))
                if schema_field_up_one_level not in valid_schemas:
                    valid_schemas.append(schema_field_up_one_level)
            except Exception as e:
                pass  # print(e.__class__, "occurred for", schema_field, "so I'm skipping it")

    bronze_table = bronze_table.select(*valid_schemas).toDF(*valid_schemas)

    return bronze_table