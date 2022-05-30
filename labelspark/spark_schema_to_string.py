'''
This code is derived  from tutorial "Working with Badly Nested Data in Spark" by by Paul Siegel
It produces a list of columns with periods between nested levels.

schema_fields_array = spark_schema_to_string(
        bronze_table.schema.jsonValue())

result looks like ['ColumnName.Category.Field1', 'ColumnName.Category.Field2',....]
'''
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