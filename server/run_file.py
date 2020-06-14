from spark import get_spark
from functions.unit_filter import unit_filter
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = get_spark()

df = spark.read.format("csv").options(header='true', delimiter = ',').load("FL_insurance_sample.csv")

properties = {'combine': "and", 
	'filter_columns': ["policyID", "statecode"], 
	'filter_values': [
		{'values': 119736, 'func': "", 'op': "eq" },
		{'values': "FL", 'func': "", 'op': "eq" }
	]
}

schema = [
	{
		'key': 'policyID',
		'datatype': 'int',
		'displayName': 'Policy Id'
	},
	{
		'key': 'statecode',
		'datatype': 'string',
		'displayName': 'State Code'
	},
	{
		'key': 'county',
		'datatype': 'string',
		'displayName': 'Country'
	},
	{
		'key': 'line',
		'datatype': 'string',
		'displayName': 'String'
	},
	{
		'key': 'construction',
		'datatype': 'string',
		'displayName': 'Construction'
	},
	{
		'key': 'point_granularity',
		'datatype': 'int',
		'displayName': 'Point Granularity'
	}
]

for d in schema:
	df = df.withColumn(d["key"], df[d["key"]].cast(d["datatype"]))

unit_filter(spark, df, properties)