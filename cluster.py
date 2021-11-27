from pyspark.sql.functions import *
import numpy as np

def cluster(df,clusteringModel):
    print("-> Entered Cluster Model Building Stage")

    X=np.array(df.select('features').collect())
    X=X.reshape(X.shape[0],X.shape[2])
    clusteringModel.partial_fit(X)
    print(clusteringModel.score(X))