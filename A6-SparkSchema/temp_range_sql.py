import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, DataFrameReader
from pyspark.sql.functions import max as sparkMax
from pyspark.sql.functions import min as sparkMin

def main(inputs, output):
    
    dataframe = spark.read.csv(inputs, schema=observation_schema)
    dataframe.createOrReplaceTempView('weather_dataframe')
    dataframe_qf = spark.sql("select * from weather_dataframe where qflag is NULL")
    dataframe_qf.createOrReplaceTempView("temp_dataframe")
    dataframe_ob = spark.sql("select * from temp_dataframe where observation in ('TMAX','TMIN')")
    #dataframe_ob.show()
    dataframe_ob.createOrReplaceTempView("selected_dataframe")

    dataframe_max = spark.sql("select date AS max_date, station AS max_station, value AS tmax from selected_dataframe WHERE observation='TMAX'")
    dataframe_max.createOrReplaceTempView("MaxDataFrame")
    #dataframe_max.show()
    dataframe_min = spark.sql("select date AS min_date, station AS min_station, value AS tmin from selected_dataframe WHERE observation='TMIN'")
    dataframe_min.createOrReplaceTempView("MinDataFrame")
    
    dataframe_join = spark.sql("select MaxDataFrame.max_date, MaxDataFrame.max_station, MaxDataFrame.tmax, MinDataFrame.tmin from MaxDataFrame INNER JOIN MinDataFrame ON ((MaxDataFrame.max_station = MinDataFrame.min_station) AND (MaxDataFrame.max_date = MinDataFrame.min_date)) ")
    dataframe_join.createOrReplaceTempView("JoinedDataFrame_1")
    #dataframe_join.show()


    dataframe_range = spark.sql("select max_date AS date, max_station AS station, (tmax-tmin)/10 AS range from JoinedDataFrame_1")
    dataframe_range.createOrReplaceTempView("JoinedDataFrame_2")
    #dataframe_range.show()

    dataframe_maxrange= spark.sql("select date, MAX(range) AS max_range from JoinedDataFrame_2 GROUP BY date ORDER BY date")
    dataframe_maxrange.createOrReplaceTempView("DataFrameMaxRange")
    #dataframe_maxrange.show(10)

    dataframe_final = spark.sql("select DataFrameMaxRange.date, JoinedDataFrame_2.station, DataFrameMaxRange.max_range from DataFrameMaxRange INNER JOIN JoinedDataFrame_2 ON ((DataFrameMaxRange.date = JoinedDataFrame_2.date) AND (DataFrameMaxRange.max_range = JoinedDataFrame_2.range)) ORDER BY DataFrameMaxRange.date, JoinedDataFrame_2.station ")
    #dataframe_final.show()

    dataframe_final.write.csv(output,mode='overwrite')

    
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
