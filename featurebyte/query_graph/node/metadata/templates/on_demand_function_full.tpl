CREATE FUNCTION {{sql_function_name}}({{sql_function_params}})
RETURNS {{sql_return_type}}
LANGUAGE PYTHON
COMMENT '{{sql_comment}}'
AS $$
{% include 'on_demand_function.tpl' %}

return {{py_function_name}}({{input_arguments}})
$$
