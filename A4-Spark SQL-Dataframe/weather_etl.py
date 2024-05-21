from pyspark.sql import SparkSession, functions, types
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark import SparkConf, SparkContext
import sys
import json
import re,string


def main(inputs,output):
   
    weather = spark.read.csv(inputs, schema=observation_schema)
    
    records_qf = weather.filter(weather['qflag'].isNull())
    records_st = records_qf.filter(weather['station'].startswith('CA'))
    records_tm = records_st.filter(weather['observation'] == ('TMAX'))
    #records_div = records_tm.(lambda r: r['value']/10)
    results = records_tm.withColumn("tmax",weather['value']/10)
    #output = results.map(lambda r: r['station'], r['date'], r['tmax']) #AttributeError: 'DataFrame' object has no attribute 'map'
    etl_data = results.select('station','date','tmax')

    etl_data.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    spark = SparkSession.builder.appName('weather ETL').getOrCreate()
    #sc = SparkContext (conf = conf)
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    inputs = sys.argv[1]
    output = sys.argv[2]

    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])
    main(inputs, output)

