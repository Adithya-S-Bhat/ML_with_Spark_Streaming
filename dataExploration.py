from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def dataExploration(df):
    print("Ham messages:",df.filter(df["Spam/Ham"]=="ham").count())
    print("Spam messages:",df.filter(df["Spam/Ham"]=="spam").count())

    length_df = df.withColumn('length',length(df['Body'])).select("Body","Spam/Ham","length")
    #length_df.show()

    meandf=length_df.groupBy('Spam/Ham').mean()
    vardf=length_df.groupBy('Spam/Ham').agg({'length':'variance'})
    vardf=vardf.withColumnRenamed("Spam/Ham","s/h")
    #uniondf=meandf.join(vardf,meandf['Spam/Ham']==vardf['s/h'],how="inner").drop('s/h').show()

    return length_df