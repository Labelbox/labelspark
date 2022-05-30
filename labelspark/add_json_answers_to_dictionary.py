import ast
import json
from labelspark.is_json import is_json

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