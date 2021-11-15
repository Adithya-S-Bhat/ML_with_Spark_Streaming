from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SQLContext,SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys

def readMyStream(rdd,schema):
  if not rdd.isEmpty():
    df = spark.read.json(rdd)
    print('Started the Process')

    newdf=spark.createDataFrame(data=spark_context.emptyRDD(),schema=schema)
    for rowNumber in range(batch_size):
      newdf=newdf.union(df.withColumn(str(rowNumber),to_json(col(str(rowNumber))))\
        .select(json_tuple(col(str(rowNumber)),"feature0","feature1","feature2"))\
          .toDF("Subject","Body","Spam/Ham"))
    newdf.show()
  

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
  stream_data.foreachRDD(lambda rdd:readMyStream(rdd,schema))

  ssc.start()
  ssc.awaitTermination()