from pyspark import SparkConf, SparkContext
import sys
import re,string

#assert sc.version >= '2.3'

# inputs = sys.argv[1]
# output = sys.argv[2]

def word_break(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    #result = re.split(wordsep,line.lower()) #string.lower()
    result = wordsep.split(line.lower())
    return result

def words_once(line):
    word = word_break(line)  #list object has no attribute 'lower'
    #word_lowercase = word.lower()
    for w in word:
        yield (w, 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

# def remove_empty(word):
#     if word[0] != "" :
#         return word
    
def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs).repartition(8)
    #text = sc.textFile(inputs)
    words = text.flatMap(words_once) #created key-value pair
    #print("log1"+str((words.take(10))))
    #print("log1:"+str((elements_filter_mapped.take(10))))
    words_filter = words.filter(lambda x: x[0] != "")
    #print("log2"+str((words_filter.take(10))))
    wordcount = words_filter.reduceByKey(add)
    #print("log3"+str((wordcount.take(10))))
    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('word count improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sys.version_info >= (3, 5)
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

