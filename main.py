from dataExploration import dataExploration
from evaluate import evaluate
from model import model
from preprocess import preprocess
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SQLContext,SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

#models
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import PassiveAggressiveClassifier

import numpy as np
import sys
import argparse

# Run using /opt/spark/bin/spark-submit main.py -host <hostname> -p <port_no> -b <batch_size> -t <isTest> -m <model_name>
parser = argparse.ArgumentParser(
    description="main driver file which calls rest of the files")
parser.add_argument('--host-name', '-host', help='Hostname', required=False,
                    type=str, default="localhost") 
parser.add_argument('--port-number', '-p', help='Port Number', required=False,
                    type=int, default=6100) 
parser.add_argument('--batch-size', '-b', help='Batch Size', required=True,
                    type=int) 
parser.add_argument('--is-test', '-t', help='Is Testing', required=False,
                    type=bool, default=False) 
parser.add_argument('--model', '-m', help='Choose Model', required=False,
                    type=str, default="NB")#model can be 'NB','SVM','LR','MLP','PA'


def readMyStream(rdd,schema,spark,classifierModel,isTest):
  if not rdd.isEmpty():
    df = spark.read.json(rdd)
    print('Started the Process')

    newdf=spark.createDataFrame(data=spark_context.emptyRDD(),schema=schema)
    for rowNumber in range(batch_size):
      newdf=newdf.union(df.withColumn(str(rowNumber),to_json(col(str(rowNumber))))\
        .select(json_tuple(col(str(rowNumber)),"feature0","feature1","feature2"))\
          .toDF("Subject","Body","Spam/Ham"))
    
    if(isTest==False):
      lengthdf=dataExploration(newdf)
      clean_df=preprocess(lengthdf)
      model(clean_df,classifierModel)
    else:
      lengthdf=dataExploration(newdf)
      clean_df=preprocess(lengthdf)
      X=np.array(clean_df.select('features').collect())
      y=np.array(clean_df.select('label').collect())
      predictions=classifierModel.predict(X.reshape(X.shape[0],X.shape[2]))
      evaluate(predictions,y.reshape(y.shape[0]))
  

if __name__ == '__main__':
  args = parser.parse_args()
  hostname=args.host_name
  port=args.port_number
  batch_size=args.batch_size
  isTest=args.is_test
  modelChosen=args.model

  spark_context = SparkContext.getOrCreate()
  spark=SparkSession(spark_context)
  ssc=StreamingContext(spark_context,10)

  stream_data=ssc.socketTextStream(hostname,int(port))

  schema=StructType(
      [StructField("Subject",StringType(),True),
      StructField("Body",StringType(),True),
      StructField("Spam/Ham",StringType(),True)])

  classifierModel=None
  if(isTest==False):
    if(modelChosen=="NB"):
      classifierModel = MultinomialNB()
    elif(modelChosen=="SVM"):
      classifierModel = SGDClassifier(alpha=0.1,n_jobs=-1,eta0=0.0,n_iter_no_change=1000)
    elif(modelChosen=="LR"):
      classifierModel = SGDClassifier(loss="log")
    elif(modelChosen=="MLP"):
      classifierModel = MLPClassifier(learning_rate='adaptive',solver="sgd",activation="logistic")
    else:
      classifierModel = PassiveAggressiveClassifier(n_jobs=-1,C=0.5,random_state=5)
  #else:get model from storage

  stream_data.foreachRDD(lambda rdd:readMyStream(rdd,schema,spark,classifierModel,isTest))

  ssc.start()
  ssc.awaitTermination()
  if(isTest==False):
    print("save the model")
  else:
    print("test metrics")