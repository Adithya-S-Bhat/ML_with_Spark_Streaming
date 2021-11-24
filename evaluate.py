from sklearn.metrics import accuracy_score,confusion_matrix

def evaluate(predictions,y_true,testingParams):

    accuracy=accuracy_score(predictions,y_true)
    print("Test Accuracy:",accuracy)

    CM=confusion_matrix(y_true,predictions)
    #since, its a binary classification 
    #0 is ham and 1 is spam -> as alphabetAsc for string indexer
    testingParams['tp']+=CM[0][0]
    testingParams['fn']+=CM[0][1]
    testingParams['fp']+=CM[1][0]
    testingParams['tn']+=CM[1][1]