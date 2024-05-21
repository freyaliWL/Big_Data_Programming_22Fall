from pyspark import SparkConf, SparkContext

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import sys
import json
import re,string


# def get_subreddit(dict):

#     key1 = dict["author"]
#     key2 = dict["score"]  
#     key3 = dict["subreddit"]
#     return (key1,key2,key3)

def main(inputs,output):
    text = sc.textFile(inputs)
    temp = text.map(json.loads)
    
    #new_rdd = temp.map(get_subreddit)
    new_rdd = temp.map(lambda d: (d["author"],d["score"],d["subreddit"]))
    #new_rdd.cache()
    #parts_e = new_rdd.filter(lambda p:'e' in p[2])
    #can not use p.subreddit
    parts_e = new_rdd.filter(lambda p:'e' in p[2]).cache()
    sub_parts_positive = parts_e.filter(lambda p: p[1]>0)
    sub_parts_negative = parts_e.filter(lambda p: p[1]<=0)

    positive_results = sub_parts_positive.map(json.dumps)
    positive_results.saveAsTextFile(output + '/positive')
    negative_results = sub_parts_negative.map(json.dumps)
    negative_results.saveAsTextFile(output + '/negative')
    
    #sql-programming-guide
    # schemaReddit = sqlContext.createDataFrame(parts_e)
    # schemaReddit.registerTempTable("subReddit")
    # subreddit_positive = sqlContext.sql("SELECT * FROM subReddit WHERE score > 0")

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext (conf = conf)
    sc.setLogLevel('WARN')
    #sqlContext = SQLContext(sc)
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
