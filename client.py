import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SQLContext,SparkSession
from pyspark.streaming import StreamingContext
#from pyspark.mllib.
import sys

hostname,port=sys.argv[1:]

spark_context = SparkContext.getOrCreate()
spark=SparkSession(spark_context)
ssc=StreamingContext(spark_context,10)

stream_data=ssc.socketTextStream(hostname,int(port))

def readMyStream(rdd):
  if not rdd.isEmpty():
    df = spark.read.json(rdd)
    print('Started the Process')
    print('Selection of Columns')
    df = df.select('transactionId','customerId','itemId','amountPaid').where(col("transactionId").isNotNull())
    df.show()


stream_data.foreachRDD( lambda rdd: readMyStream(rdd) )
stream_data.pprint()

ssc.start()
ssc.awaitTermination()
