#importing spark related libraries
from model import model
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SQLContext,SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

#importing other necessary files
from preprocess import preprocess
from helper import *

#import different model libraries
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.cluster import MiniBatchKMeans
from sklearn.cluster import Birch

#importing other necessary libraries 
import argparse
import pickle

from readStream import readStream

# joblib libraries for parallel processing of sklearn models-> uncomment if it improves performance
# from joblibspark import register_spark
# register_spark()#register joblib with spark backend

# Run using /opt/spark/bin/spark-submit main.py -host <hostname> -p <port_no> -b <batch_size> -t <isTest> -m <model_name>
parser = argparse.ArgumentParser(
    description="main driver program which calls rest of the files")
addArguments(parser)

 
if __name__ == '__main__':
  #get command line argumetns
  args = parser.parse_args()
  print(args)

  #declaration of command line argument values
  hostname=args.host_name
  port=args.port_number
  window_interval=args.window_interval
  op=args.op
  proc=args.proc
  sf=args.sampleFraction
  modelChosen=args.model
  isClustering=args.cluster
  explore=args.explore
  hashmap_size=args.hashmap_size

  #storing initial data for visualization purposes
  if(explore==True):
    spam_count_viz=[0]
    ham_count_viz=[0]
    with open('./visualizations/spam.pkl','wb') as f:
      pickle.dump(spam_count_viz,f)
      pickle.dump(ham_count_viz,f)

  #initialisation of spark context and streaming spark context
  spark_context = SparkContext.getOrCreate()
  spark=SparkSession(spark_context)
  ssc=StreamingContext(spark_context,window_interval)

  stream_data=ssc.socketTextStream(hostname,int(port))

  schema=StructType(
      [StructField("Subject",StringType(),True),
      StructField("Body",StringType(),True),
      StructField("Spam/Ham",StringType(),True)])

  classifierModel=None
  clusteringModel=None

  if(op=="train"):
    if(isClustering==False):
      if(modelChosen=="NB"):
        classifierModel = MultinomialNB()
      elif(modelChosen=="SVM"):
        classifierModel = SGDClassifier(alpha=0.1,n_jobs=-1,eta0=0.0,n_iter_no_change=1000)
      elif(modelChosen=="LR"):
        classifierModel = SGDClassifier(loss="log")
      elif(modelChosen=="MLP"):
        classifierModel = MLPClassifier(activation="logistic")
      else:
        classifierModel = PassiveAggressiveClassifier(n_jobs=-1,C=0.5,random_state=5)
    else:
      if(modelChosen=="KMeans"):
        clusteringModel = MiniBatchKMeans(n_clusters=2, random_state=123)
      else:#Birch
        clusteringModel = Birch(n_clusters=2)
  elif(op=="test"):
    if(isClustering==False):
      classifierModel = pickle.load(open(f'models/{modelChosen}', 'rb'))
    else:#cluster
      clusteringModel = pickle.load(open(f'clusteringModels/{modelChosen}', 'rb'))

  emptyRDD_count=[0]
  testingParams={'tp':0,'tn':0,'fp':0,'fn':0}
  parameters = {
    "schema":schema,
    "op":op,"proc":proc,"sf":sf,
    "hashmap_size":hashmap_size,
    "isClustering":isClustering,
    "explore" : explore
  }
  stream_data.foreachRDD(lambda rdd:readStream(rdd,ssc,\
    spark,spark_context,classifierModel,clusteringModel,\
      parameters,testingParams,emptyRDD_count))

  ssc.start()
  ssc.awaitTermination()
  
  if(op=="train"):
    if(isClustering==False):
      pickle.dump(classifierModel,open(f'models/{modelChosen}','wb'))
    else:#cluster
      pickle.dump(clusteringModel,open(f'clusteringModels/{modelChosen}','wb'))
  elif(op=="test"):
    if(isClustering==False):
      #Print test metrics
      printMetrics(testingParams,modelChosen)
    else:#cluster
      print("plot clusters")
