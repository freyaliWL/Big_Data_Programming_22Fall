import sys

from pyspark.sql import SparkSession, functions, types, DataFrameReader
from pyspark.ml.feature import VectorAssembler, SQLTransformer, StringIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(inputs, output):
    input_data = spark.read.csv(inputs, schema=tmax_schema)

    train, validation = input_data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    sql_transformer = SQLTransformer(statement = '''
    SELECT 
    dayofyear(today.date) as day_of_year, \
    today.latitude as latitude, \
    today.longitude as longitude, \
    today.elevation as elevation, \
    today.tmax as tmax, \
    yesterday.tmax as yesterday_tmax \
    FROM __THIS__ as today \
    JOIN __THIS__ as yesterday \
    ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station
    ''')

    vec_assembler = VectorAssembler(
        inputCols=['day_of_year','latitude','longitude','elevation',"yesterday_tmax"], 
        outputCol='features')

    gbt_regressor = GBTRegressor(featuresCol='features',labelCol='tmax')
    # regressor = RandomForestRegressor(featuresCol='features', labelCol='tmax')
    
    pipeline = Pipeline(stages=[sql_transformer,vec_assembler,gbt_regressor])

    model = pipeline.fit(train)
    # for Que6
    # print("---------------")
    # print(model.stages[-1].featureImportances)  # https://kb.databricks.com/en_US/machine-learning/extract-feature-info
    # print("---------------")

    predictions = model.transform(validation)
    # predictions.show()

    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', 
        labelCol='tmax', 
        metricName='r2')
    
    
    rmse_evaluator = RegressionEvaluator(
        predictionCol='prediction', 
        labelCol='tmax', 
        metricName='rmse')
    

    r2 = r2_evaluator.evaluate(predictions)
    print("r2 = " ,r2)

    rmse = rmse_evaluator.evaluate(predictions)
    print("rmse = ",rmse)
    
    model.write().overwrite().save(output)

if __name__ == '__main__':

    spark = SparkSession.builder.appName('colour prediction').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    assert spark.version >= '2.4' # make sure we have Spark 2.4+

    inputs = sys.argv[1]
    output = sys.argv[2]

    tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

    main(inputs, output)