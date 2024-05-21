import sys

from pyspark.sql import SparkSession, functions, types, DataFrameReader
from pyspark.ml.feature import VectorAssembler, SQLTransformer, StringIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import PipelineModel
import datetime

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(model_pre):
    tmax = ([('SFU_BURNABY_CAMPUS', datetime.date(2022,11,18), 49.2771, -122.9146, 330.0, 12.0),
      ('SFU_BURNABY_CAMPUS', datetime.date(2022,11,19), 49.2771, -122.9146, 330.0, 12.0)])

    tomorrow_model_df = spark.createDataFrame(tmax, schema = schema)

    tomorrow_model = PipelineModel.load(model_pre)
   
    predictions = tomorrow_model.transform(tomorrow_model_df)

    predictions.show()

if __name__ == '__main__':
   
   spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
   spark.sparkContext.setLogLevel('WARN')
   assert spark.version >= '2.3' # make sure we have Spark 2.3+
   schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])
   model_pre = sys.argv[1]
   main(model_pre)