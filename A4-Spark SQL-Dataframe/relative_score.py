from pyspark import SparkConf, SparkContext
import sys
import json
import re,string



assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_elements(elements):
    return (elements['subreddit'],(1,elements['score']))

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

def get_relative_average(joined_pair):
    #(['subreddit'],(subreddit,average_score),(score,author))
    #(['subreddit'],(score,author),average_score)
    subreddit_val = joined_pair[0]
    score_author = joined_pair[1][0]
    score_val = score_author[0]  
    author = score_author[1]
    average_score = joined_pair[1][1]
    #return relative_score
    relative_score = score_val/average_score
    return (relative_score,author)

 
def main(inputs, output):
    # main logic starts here
    text= sc.textFile(inputs)

    temp = text.map(json.loads).cache()
    
    subreddit = temp.map(get_elements)
    #print("log1: ",subreddit.collect())
    #print(subreddit.collect())

    commentbysub = temp.map( lambda c:(c['subreddit'], (c['score'],c['author'])) )

    paircount = subreddit.reduceByKey(add_pairs)

    #(['subreddit'],average_score)
    average_score_pair = paircount.map(get_average).filter(lambda x: x[1]>0)
    #join new pair, get the (A,(),())
    #print(average_score_pair.take(10))
    #('scala', 1.928939237899073)
    joinpair = commentbysub.join(average_score_pair)
    #print(joinpair.take(10))
    #('xkcd', ((3, 'spif'), 5.272939881689366))]
    relative_pair = joinpair.map(get_relative_average)
    #Finally sort in descending order (relative_score,author)
    sorted_relativepair = relative_pair.sortBy(lambda x: x[0], ascending = False)
    #sort_relativepair= sorted (relativepair, key=lambda x:x[0], reverse = True) # iterable error
    
    result_rdd = sorted_relativepair.map(json.dumps)
    result_rdd.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
