from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer,HashingTF, Tokenizer, StopWordsRemover, IDF, StringIndexer,MinMaxScaler
from pyspark.ml.feature import VectorAssembler

#n-gram

def preprocess(df,hashmap_size):
    #concatenating both body and subject into a single column
    df = df.withColumn('data',concat(col('Subject'),lit(" "),col("Body")))
    df=df.select('data','length','Spam/Ham')

    #Normalisation

    #Feature extraction
    tokenizer = Tokenizer(inputCol = 'data', outputCol = 'tokens')
    stop_remove = StopWordsRemover(inputCol = 'tokens', outputCol = 'stop_token')
    #count_vec = CountVectorizer(inputCol = 'stop_token', outputCol = 'c_vec')
    hashmap =  HashingTF(inputCol='stop_token', outputCol = 'h_vec',numFeatures=2**hashmap_size)#16384,32768
    idf = IDF(inputCol = 'h_vec', outputCol = 'tf_idf')
    ham_spam_to_numeric = StringIndexer(inputCol = 'Spam/Ham', outputCol = 'label',stringOrderType ='alphabetAsc')

    clean_up = VectorAssembler(inputCols = ['tf_idf', 'length'], outputCol = 'features')

    pipeline = Pipeline(stages=[ham_spam_to_numeric, tokenizer, stop_remove, hashmap, idf, clean_up])
    cleaner = pipeline.fit(df)
    clean_df = cleaner.transform(df)
    clean_df = clean_df.select('features','label')
    #clean_df.show(3)
    return clean_df