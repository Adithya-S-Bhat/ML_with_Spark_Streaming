from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sklearn.metrics import accuracy_score

def evaluate(predictions,y):

    # evaluator = MulticlassClassificationEvaluator()

    # accuracy=str(evaluator.evaluate(predictionsdf, {evaluator.metricName: "accuracy"}))

    # print("Test Accuracy:",accuracy)
    accuracy=accuracy_score(predictions,y)
    print("Test Accuracy:",accuracy)