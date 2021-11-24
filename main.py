#importing spark related libraries
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SQLContext,SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

#import different model libraries
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import PassiveAggressiveClassifier

#importing other necessary libraries 
import argparse
import pickle

from readStream import readStream

# Run using /opt/spark/bin/spark-submit main.py -host <hostname> -p <port_no> -b <batch_size> -t <isTest> -m <model_name>
parser = argparse.ArgumentParser(
    description="main driver file which calls rest of the files")
parser.add_argument('--host-name', '-host', help='Hostname', required=False,
                    type=str, default="localhost") 
parser.add_argument('--port-number', '-p', help='Port Number', required=False,
                    type=int, default=6100) 
parser.add_argument('--window_interval', '-w', help='Window Interval', required=False,
                    type=int, default=5) 
parser.add_argument('--op', '-op', help='Operation being performed', required=False,
                    type=str, default="train") # op can be 1 among 'train','test' or 'cluster'
parser.add_argument('--model', '-m', help='Choose Model', required=False,
                    type=str, default="NB")#model can be 1 among 'NB','SVM','LR','MLP' or 'PA'
parser.add_argument('--hashmap_size', '-hash', help='Hash map size to be used', required=False,
                    type=int, default=14)#hashmap_size=2^(this number)
                    # default and recommended hash map size on a system of 4GB RAM is 14 and on 2GB RAM it is 10. But please note that decreasing the hash map size may impact the performance of model due to collisions.

 
if __name__ == '__main__':
  args = parser.parse_args()
  print(args)
  hostname=args.host_name
  port=args.port_number
  window_interval=args.window_interval
  op=args.op
  modelChosen=args.model
  hashmap_size=args.hashmap_size

  #storing initial data for visualization purposes
  spam_count_viz=[0]
  ham_count_viz=[0]
  with open('./visualizations/spam.pkl','wb') as f:
    pickle.dump(spam_count_viz,f)
    pickle.dump(ham_count_viz,f)

  spark_context = SparkContext.getOrCreate()
  spark=SparkSession(spark_context)
  ssc=StreamingContext(spark_context,window_interval)

  stream_data=ssc.socketTextStream(hostname,int(port))

  schema=StructType(
      [StructField("Subject",StringType(),True),
      StructField("Body",StringType(),True),
      StructField("Spam/Ham",StringType(),True)])

  classifierModel=None
  if(op=="train"):
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
  elif(op=="test"):
    classifierModel = pickle.load(open(f'models/{modelChosen}', 'rb'))
  #else:#cluster

  emptyRDD_count=[0]
  stream_data.foreachRDD(lambda rdd:readStream(rdd,schema,spark,classifierModel,op,hashmap_size,emptyRDD_count,ssc,spark_context))

  ssc.start()
  ssc.awaitTermination()
  
  if(op=="train"):
    pickle.dump(classifierModel,open(f'models/{modelChosen}','wb'))
  elif(op=="test"):
    print("test metrics")