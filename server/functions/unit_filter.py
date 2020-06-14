filter_columns_key = "filter_columns"
filter_values_prop_key = "filter_values"
filter_values_key = "values"

def unit_filter(spark, df, properties):
	if(len(properties) == 0):
		raise Exception('Please specify the propeeties')
	
	datatype_dict = {x[0]: x[1] for x in df.dtypes}
	filter_string = get_filter_string(properties, datatype_dict)
	df = df.filter(filter_string)
	return df

def get_filter_string(properties, datatype_dict):
	filter_values = []
	for filter_column, filter_value_prop in zip(properties[filter_columns_key], properties[filter_values_prop_key]):
		filter_values.append(f'{get_filter_condition(filter_column, filter_value_prop[filter_values_key], filter_value_prop["op"], datatype_dict[filter_column])}')

	return f' {properties["combine"]} '.join(filter_values)

def get_filter_condition(filter_column, filter_value, op, datatype):
	return f'{filter_column} {get_operator(op)} {format_field(filter_value, datatype)}'

def get_operator(op):
	if op == "eq":
		return "=="

def format_field(value, datatype):
	if datatype == "string":
		if type(value) == str:
			return f'"{value}"'
		elif type(value) == list:
			return "(" + ', '.join("'{0}'".format(x) for x in a) + ")"

	elif datatype == "int":
		if type(value) == int:
			return value
		elif type(value) == int:
			return f'({",".join([str(x) for x in value])})'