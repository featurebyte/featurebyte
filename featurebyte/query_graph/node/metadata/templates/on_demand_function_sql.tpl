CREATE FUNCTION {{sql_function_name}}({{sql_function_params}})
RETURNS {{sql_return_type}}
LANGUAGE PYTHON
COMMENT '{{sql_comment}}'
AS $$
{{ py_function_body }}

output = {{py_function_name}}({{input_arguments}})
return None if pd.isnull(output) else output
$$
