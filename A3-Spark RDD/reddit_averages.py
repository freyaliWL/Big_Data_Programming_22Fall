from pyspark import SparkConf, SparkContext
import sys
import json
import re,string

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_elements(elements):
    return (elements['subreddit'],(1,elements['score']))

#def get_inputs(elements):
#    return json.loads(elements)

#def get_outputs(elements):
#    return json.dumps(elements)

def add_pairs(a,b):
    count = a[0] + b[0]
    score_sum = a[1] + b[1]
    return (count,score_sum)
   
def get_average(a):
    average = int(a[1][1])/int(a[1][0])
    return (a[0],average)

def get_key(kv):
    return kv[0]

def output_format(kv):
    #k,v = kv
    return '%s %i' % (kv[0], kv[1])
 
def main(inputs, output):
    # main logic starts here
    text= sc.textFile(inputs)

    temp = text.map(json.loads)

    subreddit = temp.map(get_elements)
    #print("log1: ",subreddit.collect())
    #print(subreddit.collect())
    result = subreddit.reduceByKey(add_pairs)
    
    #print("log2: ",result.collect())
    result_average = result.map(get_average)
    #print("log3: ",result_average.collect())
    result_average_rdd = result_average.map(json.dumps)

    #outdata = result_average_rdd.sortBy(get_key).map(output_format)

    result_average_rdd.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
