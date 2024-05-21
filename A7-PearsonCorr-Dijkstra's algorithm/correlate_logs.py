import sys
import re
from math import sqrt
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, DataFrameReader
from pyspark.sql.functions import sum as sparkSum
from pyspark.sql.functions import count as sparkCount
from pyspark.sql.functions import pow as sparkPow

def get_row(lines):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    temp = re.match(line_re, lines)
    #return temp
    if temp:
        pairs = line_re.split(lines)
        return (pairs[1], int(pairs[4]))
    else:
        return None


def main(inputs, output):
    loglines = spark.sparkContext.textFile(inputs)
    rddlines = loglines.map(get_row).filter(lambda x: x is not None)
    dataframe = spark.createDataFrame(rddlines, schema = web_schema)
    #dataframe.show()
    df_TotalBytes = dataframe.groupby('host').agg(sparkSum('byte').alias('TotalBytes'))
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
    print("r = %g\nr^2 = %g" % (r,r2))

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
    web_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('byte', types.IntegerType())
    ])

    main(inputs,output)