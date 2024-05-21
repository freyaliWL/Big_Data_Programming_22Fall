import re, string
from pyspark import SparkConf, SparkContext
import sys
inputs = sys.argv[1]
output = sys.argv[2]
conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+
def separator(line):
    element = line.split()
    return tuple(element)
def filterator(elements):
    if (elements[1] == "en" and elements[2] != "Main_Page" and not elements[2].startswith('Special:')):
        return elements
def get_elements(elements):
    return elements[0], (int(elements[3]), elements[2])
def get_max(x,y):
   return max(x,y)
def get_key(kv):
    return kv[0]
def tab_separated(kv):
    return '%s\t%s' % (kv[0], kv[1])
text = sc.textFile(inputs)
elements = text.map(separator)
elements_filter = elements.filter(filterator)
elements_filter_mapped = elements_filter.map(get_elements)
#print("log1:"+str((elements_filter_mapped.take(10))))
max_count = elements_filter_mapped.reduceByKey(get_max)
#print("log2:"+str((max_count).take(10)))
outdata = max_count.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)

