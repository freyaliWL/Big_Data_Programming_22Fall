import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, DataFrameReader
from pyspark.sql.functions import pow as sparkPow
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions

def main(inputs):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe',inputs).load()
    value = messages.select(messages['value'].cast('string'))
    value_df = value.select(functions.split(value['value'], " ").getItem(0).alias("x"),functions.split(value['value'], " ").getItem(1).alias("y"),functions.lit(1).alias('n'))
    df_xy = value_df.withColumn('x_y',value_df.x*value_df.y)
    df_powx = df_xy.withColumn('x2',sparkPow('x',2))
    df_temp = df_powx.agg(functions.sum(df_powx['x']),functions.sum(df_powx['y']),functions.sum(df_powx['x_y']),functions.sum(df_powx['x2']),functions.sum(df_powx['n']))
    df_calculated = df_temp.withColumn('slope', (df_temp['sum(x_y)'] - (1/df_temp['sum(n)'] * df_temp['sum(x)'] * df_temp['sum(y)'])) /
                                      (df_temp['sum(x2)'] - (1/df_temp['sum(n)'] * (df_temp['sum(x)'] ** 2)))) 
    final_df = df_calculated.withColumn('intercept', (df_temp['sum(y)'] / df_temp['sum(n)']) - (df_calculated['slope'] * (df_temp['sum(x)'] / df_temp['sum(n)']))) \
    .select('slope','intercept')

    stream = final_df.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(60)

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Spark Streaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    inputs = sys.argv[1]
    main(inputs)