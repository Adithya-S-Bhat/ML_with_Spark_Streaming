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
import sys
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import PassiveAggressiveClassifier
import numpy as np

def readMyStream(rdd,schema,spark,classifierModel):
  if not rdd.isEmpty():
    df = spark.read.json(rdd)
    print('Started the Process')

    newdf=spark.createDataFrame(data=spark_context.emptyRDD(),schema=schema)
    for rowNumber in range(batch_size):
      newdf=newdf.union(df.withColumn(str(rowNumber),to_json(col(str(rowNumber))))\
        .select(json_tuple(col(str(rowNumber)),"feature0","feature1","feature2"))\
          .toDF("Subject","Body","Spam/Ham"))

    lengthdf=dataExploration(newdf)
    clean_df=preprocess(lengthdf)
    model(clean_df,classifierModel)
    # X=np.array(clean_df.select('features').collect())
    # y=np.array(clean_df.select('label').collect())
    # predictions=classifierModel.predict(X.reshape(X.shape[0],X.shape[2]))
    # evaluate(predictions,y.reshape(y.shape[0]))
  

if __name__ == '__main__':
  hostname,port,batch_size=sys.argv[1:]
  batch_size=int(batch_size)

  spark_context = SparkContext.getOrCreate()
  spark=SparkSession(spark_context)
  ssc=StreamingContext(spark_context,10)

  stream_data=ssc.socketTextStream(hostname,int(port))

  schema=StructType(
      [StructField("Subject",StringType(),True),
      StructField("Body",StringType(),True),
      StructField("Spam/Ham",StringType(),True)])
  #classifierModel = MultinomialNB()
  #classifierModel = SGDClassifier(alpha=0.1,n_jobs=-1,eta0=0.0,n_iter_no_change=1000)
  #classifierModel = SGDClassifier(loss="log")
  #classifierModel = MLPClassifier(learning_rate='adaptive',solver="sgd",activation="logistic")
  classifierModel = PassiveAggressiveClassifier(n_jobs=-1,C=0.5,random_state=5)

  stream_data.foreachRDD(lambda rdd:readMyStream(rdd,schema,spark,classifierModel))

  ssc.start()
  ssc.awaitTermination()