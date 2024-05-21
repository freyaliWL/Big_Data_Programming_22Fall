import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, DataFrameReader

def separator(line):
    pairs_list=line.split(':')
    return pairs_list

def main(inputs, output, source, endnode):
    line = sc.textFile(inputs + '/links-simple-sorted.txt')
    initial_data = [{"node":source,"source":"-1","distance":int(0)}]
    pair_rdd = line.flatMap(get_rdd)

    dataframe = spark.createDataFrame(pair_rdd, schema = path_schema).cache()
    #dataframe.show()
    current_graph = spark.createDataFrame(initial_data, schema = bfs_schema)
    #current_graph.show()

    for i in range(6):
        source_graph = current_graph.where(current_graph['distance']==i)
        #current_graph.show()
        joined_graph = source_graph.join(dataframe, on='node')
        # node source distance endnode
        # joined_graph.show()
        # new_node=endnode source=node distance=i+1 -> current_graph
        new_paths = joined_graph.withColumn('source', joined_graph.node).withColumn('distance', joined_graph.distance + 1).select('endnode', 'source', 'distance').withColumnRenamed('endnode', 'node')
        # update current_graph
        current_graph = current_graph.unionAll(new_paths)

        current_graph.write.csv(output + '/iter-' + str(i), mode = 'overwrite')

        if joined_graph.filter(joined_graph.endnode == endnode).count() != 0:
            break

    # current_graph remove duplicates

    # endnode -> source
    path = [endnode]
    cur = current_graph.where(current_graph.node == endnode).select(current_graph.source).first()[0]
    #print(cur)
    #print(type(cur))
    #dataframe cur change to data
    while int(cur) != source:
        path.append(cur)
        cur = current_graph.where(current_graph.node == cur).select(current_graph.source).first()[0]
    path.append(source)
    path = reversed(path)

    sc.parallelize(path).saveAsTextFile(output + '/path')


def get_rdd(line):
 
    pairs_list=separator(line)
    #nodeslist=breaker(pairs_list)
    nodeslist = pairs_list[1].split(' ')
    for elements in nodeslist:
        if elements!='':
            yield (int(pairs_list[0]),int(elements))

if __name__ == '__main__':
    
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = int(sys.argv[3])
    endnode = int(sys.argv[4])
    spark = SparkSession.builder.appName('shortest path').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
    path_schema = types.StructType([
        types.StructField('node', types.StringType()),
        types.StructField('endnode',types.StringType()),
    ])
    bfs_schema = types.StructType([
        types.StructField('node', types.StringType()),
        types.StructField('source', types.StringType()),
        types.StructField('distance', types.IntegerType())
    ])

    main(inputs,output,source,endnode)