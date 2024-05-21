from cassandra.cluster import Cluster
import sys,os,gzip
import re
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, DataFrameReader
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import uuid

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def seperate_lines(lines):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    temp = re.match(line_re, lines)
    #return temp
    if temp:
        pairs = line_re.split(lines)
        uid = str(uuid.uuid4())
        return (pairs[1], uid, int(pairs[4]))
    else:
        return None

def main(inputs, keyspace, table):

    #loglines = spark.sparkContext.textFile(inputs).repartition()
    loglines = spark.sparkContext.textFile(inputs)
    rddlines = loglines.map(seperate_lines).filter(lambda x: x is not None)

    dataframe = spark.createDataFrame(rddlines, schema = web_schema).repartition(10)
    #dataframe.show()
    session.execute('CREATE KEYSPACE IF NOT EXISTS '+keyspace \
    +" WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2};")
    session.set_keyspace(keyspace)
    session.execute('DROP TABLE IF EXISTS ' +keyspace+'.'+table)
    session.execute('create table ' +table+ '(host TEXT, id TEXT, bytes INT, PRIMARY KEY(host, id));')
    
    dataframe.write.format("org.apache.spark.sql.cassandra").mode('overwrite') \
         .options(table=table, keyspace=keyspace)\
         .options(**{'confirm.truncate':True})\
         .save()
    
if __name__ == '__main__':
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('spark load logs') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]

    session = Cluster(cluster_seeds).connect(keyspace)

    web_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('id', types.StringType()),
        types.StructField('bytes', types.IntegerType())
    ])

    main(inputs, keyspace, table)