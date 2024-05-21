from pyspark import SparkConf, SparkContext
import sys
import random


# add more functions as necessary
def iterator(iterations):
    random.seed()
    count = 0
    #for i in range(len(iterations)):   # get count based on list size
    for i in range(iterations):
        sum = 0.0
        while (sum<1):
            sum = sum + random.random()
            count = count+1

    return count


def main(inputs):

    # main logic starts here
    batches = 10000
    rdd_total_count = 0 #total iterations
    #samples = inputs
    samples = int(inputs) #change string to list
    #sample_once = samples/batches #changed samples type

    #rdd = sc.parallelize([samples],batches).glom()   #glom() coalescing all elements within each partition into list
   
    rdd = sc.parallelize([samples],batches)
           
    rdd_total_count = rdd.map(iterator)
    rdd_sum = rdd_total_count.reduce(lambda x,y: x+y)
    #print("rdd_sum is",rdd_sum)
    output = int(rdd_sum)/samples #list index out of range
    #print("The output is: {}".format(output))
    print(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    assert sc.version >= '3.0'
    inputs = sys.argv[1]
    #output = sys.argv[2]
    main(inputs)