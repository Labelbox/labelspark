import json
import urllib
import databricks.koalas as pd
import ast
from labelbox import Client

#spark specific stuff
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql import Row


# upload spark dataframe to Labelbox
def create_dataset(client, spark_dataframe, dataset_name="Default"):
    # expects spark dataframe to have two columns: external_id, row_data
    # external_id is the asset name ex: "photo.jpg"
    # row_data is the URL to the asset
    spark_dataframe = spark_dataframe.to_koalas()
    dataSet_new = client.create_dataset(name=dataset_name)

    # ported Pandas code to koalas
    data_row_urls = [{
        "external_id": row['external_id'],
        "row_data": row['row_data']
    } for index, row in spark_dataframe.iterrows()]
    upload_task = dataSet_new.create_data_rows(data_row_urls)
    upload_task.wait_till_done()
    print("Dataset created in Labelbox.")
    return dataSet_new


# returns raw bronze annotations
def get_annotations(client, project_id, spark, sc):
    project = client.get_project(project_id)
    with urllib.request.urlopen(project.export_labels()) as url:
        api_response_string = url.read().decode()  # this is a string of JSONs
    bronze_table = jsonToDataFrame(api_response_string, spark, sc)
    bronze_table = dataframe_schema_enrichment(bronze_table)
    return bronze_table


import requests  #we need to handle a large frame-by-frame dataset for videos, so we use requests


def get_videoframe_annotations(bronze_video_labels, api_key, spark, sc):
    # This method takes in the bronze table from get_annotations and produces
    # an array of bronze dataframes containing frame labels for each project
    bronze_video_labels = bronze_video_labels.withColumnRenamed(
        "DataRow ID", "DataRowID")
    koalas_bronze = bronze_video_labels.to_koalas()

    # We manually build a string of frame responses to leverage our existing jsonToDataFrame code, which takes in JSON
    headers = {'Authorization': f"Bearer {api_key}"}
    master_array_of_json_arrays = []
    for index, row in koalas_bronze.iterrows():
        response = requests.get(row.Label.frames, headers=headers, stream=False)
        data = []
        for line in response.iter_lines():
            data.append({
                "DataRow ID": row.DataRowID,
                "Label": json.loads(line.decode('utf-8'))
            })
        massive_string_of_responses = json.dumps(data)
        master_array_of_json_arrays.append(massive_string_of_responses)

    array_of_bronze_dataframes = []
    for frameset in master_array_of_json_arrays:
        array_of_bronze_dataframes.append(jsonToDataFrame(frameset, spark, sc))

    return array_of_bronze_dataframes


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


# processes bronze table into silver table
def bronze_to_silver(bronze_table):
    bronze_table = flatten_bronze_table(bronze_table)
    bronze_table = bronze_table.withColumnRenamed("DataRow ID", "DataRowID")

    # video labels need special handling
    video = False
    if "Label.frameNumber" in bronze_table.columns:
        video = True
    if video:
        bronze_table = bronze_table.withColumnRenamed("Label.frameNumber",
                                                      "frameNumber")
    bronze_table = bronze_table.to_koalas()

    new_json = []
    for index, row in bronze_table.iterrows():
        my_dictionary = {}

        # classifications
        try:  # this won't work if there are no classifications
            for index, title in enumerate(row["Label.classifications.title"]):
                if "Label.classifications.answer" in row:
                    if row["Label.classifications.answer"][
                            index] is not None:  # if answer is null, that means it exists in secondary "answers" column
                        answer = row["Label.classifications.answer"][index]
                    else:
                        answer = row["Label.classifications.answers"][
                            index]  # it must be a checklist if .answer is None
                else:
                    answer = row["Label.classifications.answer.title"][index]
                my_dictionary = add_json_answers_to_dictionary(
                    title, answer, my_dictionary)
        except Exception as e:
            print("No classifications found.")

        # object counting
        try:  # this field won't work if the Label does not have objects in it
            for object in row.get("Label.objects.title", []):
                object_name = '{}.count'.format(object)
                if object_name not in my_dictionary:
                    my_dictionary[object_name] = 1  # initialize with 1
                else:
                    my_dictionary[object_name] += 1  # add 1 to counter
        except Exception as e:
            print("No objects found.")

        my_dictionary["DataRowID"] = row.DataRowID  # close it out
        if video:
            my_dictionary[
                "frameNumber"] = row.frameNumber  # need to store the unique framenumber identifier for video
        new_json.append(my_dictionary)

    parsed_classifications = pd.DataFrame(new_json).to_spark()

    bronze_table = bronze_table.to_spark()
    if video:
        # need to inner-join with frameNumber to avoid creating N-squared datarows, since each frame has same DataRowID
        joined_df = parsed_classifications.join(bronze_table,
                                                ["DataRowID", "frameNumber"],
                                                "inner")
    else:
        joined_df = parsed_classifications.join(bronze_table, ["DataRowID"],
                                                "inner")

    return joined_df.withColumnRenamed("DataRowID", "DataRow ID")


def jsonToDataFrame(json, spark, sc, schema=None):
    # code taken from Databricks tutorial https://docs.azuredatabricks.net/_static/notebooks/transform-complex-data-types-python.html
    reader = spark.read
    if schema:
        reader.schema(schema)
    return reader.json(sc.parallelize([json]))


# this LB-specific method converts schema to proper format based on common field names in our JSON

LABELBOX_DEFAULT_TYPE_DICTIONARY = {
    'Agreement': 'integer',
    'Benchmark Agreement': 'integer',
    'Created At': 'timestamp',
    'Updated At': 'timestamp',
    'Has Open Issues': 'integer',
    'Seconds to Label': 'float',
}


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


def spark_schema_to_string(schema, progress=''):
    if schema['type'] == 'struct':
        for field in schema['fields']:
            key = field['name']
            yield from spark_schema_to_string(field, f'{progress}.{key}')
    elif schema['type'] == 'array':
        if type(schema['elementType']) == dict:
            yield from spark_schema_to_string(schema['elementType'], progress)
        else:
            yield progress.strip('.')
    elif type(schema['type']) == dict:
        yield from spark_schema_to_string(schema['type'], progress)
    else:
        yield progress.strip('.')


def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except Exception as e:
        return False
    return True


def add_json_answers_to_dictionary(title, answer, my_dictionary):
    try:  # see if I can read the answer string as a literal --it might be an array of JSONs
        convert_from_literal_string = ast.literal_eval(answer)
        if isinstance(convert_from_literal_string, list):
            for item in convert_from_literal_string:  # should handle multiple items
                my_dictionary = add_json_answers_to_dictionary(
                    title, item, my_dictionary)
            # recursive call to get into the array of arrays
    except Exception as e:
        pass

    if is_json(
            answer
    ):  # sometimes the answer is a JSON string; this happens when you have nested classifications
        parsed_answer = json.loads(answer)
        try:
            answer = parsed_answer["title"]
        except Exception as e:
            pass

    # this runs if the literal stuff didn't run and it's not json
    list_of_answers = []
    if isinstance(answer, list):
        for item in answer:
            list_of_answers.append(item["title"])
        answer = ",".join(list_of_answers)
    elif isinstance(answer, dict):
        answer = answer["title"]  # the value is in the title

    # Perform check to make sure we do not overwrite a column
    if title not in my_dictionary:
        my_dictionary[title] = answer

    return my_dictionary
