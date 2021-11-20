from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def dataExploration(df):
    print("----------------------------") #looks good 
    ham_count=df.filter(df["Spam/Ham"]=="ham").count()
    spam_count=df.filter(df["Spam/Ham"]=="spam").count()
    total_count=ham_count+spam_count
    ham_percent=ham_count/total_count
    spam_percent=spam_count/total_count
    print("Ham messages:",ham_count," (",ham_percent*100,"%)")
    print("Spam messages:",spam_count," (",spam_percent*100,"%)")

    length_df = df.withColumn('length',length(df['Body'])).select("Body","Spam/Ham","length")
    #length_df.show()

    meandf=length_df.groupBy('Spam/Ham').mean()
    vardf=length_df.groupBy('Spam/Ham').agg({'length':'variance'})
    vardf=vardf.withColumnRenamed("Spam/Ham","s/h")
    #uniondf=meandf.join(vardf,meandf['Spam/Ham']==vardf['s/h'],how="inner").drop('s/h').show()

    return length_df