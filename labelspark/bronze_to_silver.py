#this code block is needed for backwards compatibility with older Spark versions
from pyspark import SparkContext
from packaging import version
try:
  import pyspark.pandas as pd
except:
  import databricks.koalas as pd 

from labelspark.flatten_bronze_table import flatten_bronze_table
from labelspark.add_json_answers_to_dictionary import add_json_answers_to_dictionary

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
    bronze_table = bronze_table.to_pandas_on_spark()

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
