from pyspark.sql.functions import *
import numpy as np

def model(df,classifierModel):
    X=np.array(df.select('features').collect())
    y=np.array(df.select('label').collect())
    print("Shape of X:",X.shape)
    X=X.reshape(X.shape[0],X.shape[2])
    print("Shape of y:",y.shape)
    y=y.reshape(y.shape[0])
    classifierModel.partial_fit(X,y,classes=list(range(2)))