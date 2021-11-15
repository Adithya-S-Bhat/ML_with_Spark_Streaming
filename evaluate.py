from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def evaluate(predictionsdf):
    evaluator = MulticlassClassificationEvaluator()

    accuracy=str(evaluator.evaluate(predictionsdf, {evaluator.metricName: "accuracy"}))

    print("Test Accuracy:",accuracy)