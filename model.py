from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes

def model(df):
    nb = NaiveBayes()
    spam_detector = nb.fit(df)

    return spam_detector