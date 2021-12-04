from pyspark.sql.functions import *
import numpy as np

from sklearn.utils import parallel_backend
from sklearn.decomposition import TruncatedSVD

def cluster(df,clusteringModel,chosenModel):
    print("-> Entered Cluster Model Building Stage")

    #reshaping
    X=np.array(df.select('features').collect())
    print("Shape of X:",X.shape)
    X=np.reshape(X,(X.shape[0],X.shape[2]))

    tsvd = TruncatedSVD(n_components=10)
    tsvd_result = tsvd.fit_transform(X[:,:-1])
    print(f"Cumulative variance explained by the selected components: {np.sum(tsvd.explained_variance_ratio_)}")

    #with parallel_backend('spark',n_jobs=-1):
    clusteringModel.partial_fit(tsvd_result)
    if(chosenModel=='KMeans'):
        print(clusteringModel.score(X))