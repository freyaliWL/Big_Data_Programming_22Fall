import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, DataFrameReader
from pyspark.sql.functions import max as sparkMax

# add more functions as necessary

def main(inputs, output):
    
    df = spark.read.csv(inputs, schema=wiki_schema, sep=' ').withColumn('filename',path_to_hour(functions.input_file_name()))
   
    df_1 = df.where(df.title != functions.lit('Main_Page'))
    
    #df_2 = df_1.where(df_1.title != functions.lit('Special:'))
    df_2 = df_1.where(~df_1.title.startswith('Special:'))

    sub_wiki = df_2.where(df_2.language == functions.lit('en'))
    #sub_wiki.show()
    
    sub_wiki_maxcount = sub_wiki.groupby(sub_wiki["filename"].alias('hours')).agg(sparkMax(sub_wiki["views"]).alias('MAX'))
    #sub_wiki_maxcount = sub_wiki_maxcount.withColumnRenamed('filename','hours')
    #sub_wiki_maxcount.show()
    
    joined_wiki = sub_wiki_maxcount.join(sub_wiki,(sub_wiki["filename"]==sub_wiki_maxcount["hours"]) & (sub_wiki["views"]==sub_wiki_maxcount["MAX"]))
    #joined_wiki.show()
    
    final_wiki = joined_wiki.select('hours','title','views')
    
    output_wiki = final_wiki.orderBy('hours','title')
    output_wiki.write.json(output, mode = 'overwrite')


@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    new_path = path.split('/')
    element_last = new_path[-1]
    'YYMMDD-HH'
    component_last = element_last[11:22]
    return component_last

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
    wiki_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.LongType()),
    types.StructField('bytes', types.StringType()),
])
    main(inputs, output)

    
