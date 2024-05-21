
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re,os,gzip
import uuid
from math import sqrt
from pyspark.sql import SparkSession, functions, types, DataFrameReader
from pyspark.sql.functions import sum as sparkSum
from pyspark.sql.functions import count as sparkCount
from pyspark.sql.functions import pow as sparkPow

cluster_seeds = ['node1.local', 'node2.local']
spark = SparkSession.builder.appName('Spark Cassandra example') .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
#spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main(keyspace,table):
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    dataframe = df.select('host','bytes')
    dataframe.show()
    df_TotalBytes = dataframe.groupby('host').agg(sparkSum('bytes').alias('TotalBytes'))
    df_NumRequests = dataframe.groupby('host').agg(sparkCount('host').alias('NumRequests'))
    joined_df = df_NumRequests.join(df_TotalBytes,'host')
    #joined_df.show()
    df_1 = joined_df.withColumn('xy',joined_df.NumRequests * joined_df.TotalBytes)
    df_2 = df_1.withColumn('x2',sparkPow(joined_df.NumRequests,2))
    df_3 = df_2.withColumn('y2',sparkPow(joined_df.TotalBytes,2))
    #df_3.show()
    df_4 = df_3.withColumnRenamed('NumRequests','x')
    df_5 = df_4.withColumnRenamed('TotalBytes','y')
    #df_5.show()

    n = df_5.groupby().agg(sparkCount('host')).first()[0]
    #n.show()
    df_agg = df_5.select('x','y','xy','x2','y2').groupby().sum()
    #df_agg.show()
    x_sum = df_agg.select('sum(x)').first()[0]
    y_sum = df_agg.select('sum(y)').first()[0]
    xy_sum = df_agg.select('sum(xy)').first()[0]
    x2_sum = df_agg.select('sum(x2)').first()[0]
    y2_sum = df_agg.select('sum(y2)').first()[0]
    #print("__________sdfghjkhgfds__________")
    #print(x,y,xy,x2,y2)
    r = (n*xy_sum - x_sum*y_sum)/(sqrt(n*x2_sum-x_sum**2)*sqrt(n*y2_sum-y_sum**2))
    r2 = r**2

    print("____________________________________________")
    print("r = %g\nr^2 = %g" % (r,r2))

if __name__ == '__main__':
    table = sys.argv[2]
    keyspace = sys.argv[1]
  
    main(keyspace,table)