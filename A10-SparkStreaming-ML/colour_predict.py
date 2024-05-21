import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


from colour_tools import colour_schema, rgb2lab_query, plot_predictions
from pyspark.sql import SparkSession,functions,types
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main(inputs):
    input_data = spark.read.csv(inputs, schema=colour_schema)
    index = StringIndexer(inputCol="word", outputCol="indexed")

    train, validation = input_data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    #creat model
    rgb_assembler = VectorAssembler(inputCols=['R','G','B'],outputCol='features')
    
    classifier = MultilayerPerceptronClassifier(layers=[3, 30, 11],featuresCol='features',labelCol='indexed')
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol='indexed')

    rgb_pipeline = Pipeline(stages=[rgb_assembler, index, classifier])
    rgb_model = rgb_pipeline.fit(train)
    rgb_predictions = rgb_model.transform(validation)
    #predictions.show()
    plot_predictions(rgb_model,'RGB',labelCol='word')
   
    #rgb_evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol='indexed')
    rgb_score = evaluator.evaluate(rgb_predictions)
    print('Validation score for RGB model: %g' % (rgb_score,))

    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sql_transformer = SQLTransformer(statement = rgb_to_lab_query)
    lab_assembler = VectorAssembler(inputCols=['labL', 'labA', 'labB'],outputCol='features')
    lab_pipeline = Pipeline(stages=[sql_transformer,lab_assembler,index,classifier])

    lab_model = lab_pipeline.fit(train)
    plot_predictions(lab_model, 'LAB', labelCol='word')
    lab_predictions = lab_model.transform(validation)
    #lab_evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol='indexed')
    lab_score = evaluator.evaluate(lab_predictions)
    print('Validation score for LAB model: %g' % (lab_score,))

if __name__ == '__main__':

    spark = SparkSession.builder.appName('colour prediction').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    assert spark.version >= '3.0'
    inputs = sys.argv[1]

    main(inputs)