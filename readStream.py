#importing spark related libraries
from pyspark.sql.functions import *

#importing other necessary libraries 
import numpy as np


#importing necessary files
from dataExploration import dataExploration
from evaluate import evaluate
from model import model
from preprocess import preprocess
from cluster import cluster

def readStream(rdd,schema,spark,classifierModel,clusteringModel,op,hashmap_size,emptyRDD_count,ssc,spark_context,testingParams,parallel_backend):
  if not rdd.isEmpty():
    emptyRDD_count[0]=0

    #parsing json to get a df
    df = spark.read.json(rdd)
    print('\nStarted Processing a Batch')

    # applying schema to it
    newdf=spark.createDataFrame(data=spark_context.emptyRDD(),schema=schema)
    n_samples = len(df.columns)
    for rowNumber in range(n_samples):
      newdf=newdf.union(df.withColumn(str(rowNumber),to_json(col(str(rowNumber))))\
        .select(json_tuple(col(str(rowNumber)),"feature0","feature1","feature2"))\
          .toDF("Subject","Body","Spam/Ham"))
    
    if(op=="train"):
      lengthdf=dataExploration(newdf)
      clean_df=preprocess(lengthdf,hashmap_size)
      model(clean_df,classifierModel,parallel_backend)
      X=np.array(clean_df.select('features').collect())
      y=np.array(clean_df.select('label').collect())
      predictions=classifierModel.predict(X.reshape(X.shape[0],X.shape[2]))
      evaluate(predictions,y.reshape(y.shape[0]),testingParams)
    elif(op=="test"):
      lengthdf=dataExploration(newdf)
      clean_df=preprocess(lengthdf,hashmap_size)
      X=np.array(clean_df.select('features').collect())
      y=np.array(clean_df.select('label').collect())
      predictions=classifierModel.predict(X.reshape(X.shape[0],X.shape[2]))
      evaluate(predictions,y.reshape(y.shape[0]),testingParams)
    else:#cluster
      lengthdf=dataExploration(newdf)
      clean_df=preprocess(lengthdf,hashmap_size)
      cluster(clean_df,clusteringModel)

  else:#rdd is empty
    emptyRDD_count[0]+=1
    if(emptyRDD_count[0]==3):#if 3 empty rdds are received, assume streaming has stopped
      ssc.stop()