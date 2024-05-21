import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, DataFrameReader
from pyspark.sql.functions import max as sparkMax
from pyspark.sql.functions import min as sparkMin

def main(inputs, output):
    
    weather = spark.read.csv(inputs, schema=observation_schema)

    weather_max_min_p0 = weather.filter(weather['qflag'].isNull())
    weather_max_p0 = weather_max_min_p0.filter(weather['station'].startswith('CA'))
    #get max min column in two dfs
    #weather_max_p0.show()
    weather_min = weather_max_min_p0.filter(weather['observation'] == ('TMIN'))
    weather_max = weather_max_min_p0.filter(weather['observation'] == ('TMAX'))
    weather_temp_min = weather_min.groupBy(weather_min['station'].alias('min_station'), weather_min['date'].alias('min_date')).agg(sparkMin(weather_min['value']).alias('tmin'))
    weather_temp_max = weather_max.groupBy(weather_max['station'].alias('max_station'), weather_max['date'].alias('max_date')).agg(sparkMax(weather_max['value']).alias('tmax'))
    #weather_temp_min.show()
    #weather_temp_max.show()
    weather_join = weather_temp_max.join(weather_temp_min,(weather_temp_min['min_station']==weather_temp_max['max_station']) & (weather_temp_min['min_date']==weather_temp_max['max_date']))
    #weather_join.show()

    weather_join_ranges = weather_join.withColumn('ranges',((weather_join.tmax-weather_join.tmin)/10))
    weather_join_ranges_select = weather_join_ranges.select(weather_join.max_date, weather_join.max_station.alias("station"),weather_join_ranges.ranges).cache()
    #weather_join_ranges_select.show()
    weather_maxranges = weather_join_ranges_select.groupBy(weather_join_ranges_select.max_date.alias('date')).agg(sparkMax(weather_join_ranges_select.ranges).alias('max_range'))
    weather_final = weather_maxranges.join(weather_join_ranges_select,(weather_join_ranges_select['max_date']==weather_maxranges['date']) & (weather_join_ranges_select['ranges']==weather_maxranges['max_range']))
    #weather_final= weather_join_maxranges.orderBy(weather_join_ranges.max_date, weather_join_ranges.max_station)
    #weather_final.show()
    weather_output = weather_final.select("date","station","max_range").sort("date","station")
    #weather_output.show()
    weather_output.write.csv(output,mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
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
