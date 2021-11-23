from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sklearn.metrics import accuracy_score,confusion_matrix

def evaluate(predictions,y_true,testingParams):

    # evaluator = MulticlassClassificationEvaluator()

    # accuracy=str(evaluator.evaluate(predictionsdf, {evaluator.metricName: "accuracy"}))

    # print("Test Accuracy:",accuracy)
    accuracy=accuracy_score(predictions,y_true)
    print("Test Accuracy:",accuracy)

    CM=confusion_matrix(y_true,predictions)
    #since, its a binary classification 
    testingParams['tp']+=CM[0][0]
    testingParams['fn']+=CM[0][1]
    testingParams['fp']+=CM[1][0]
    testingParams['tn']+=CM[1][1]
