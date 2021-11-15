from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, Tokenizer, StopWordsRemover, IDF, StringIndexer
from pyspark.ml.feature import VectorAssembler


def preprocess(df):
    tokenizer = Tokenizer(inputCol = 'Body', outputCol = 'tokens')
    stop_remove = StopWordsRemover(inputCol = 'tokens', outputCol = 'stop_token')
    count_vec = CountVectorizer(inputCol = 'stop_token', outputCol = 'c_vec')
    idf = IDF(inputCol = 'c_vec', outputCol = 'tf_idf')
    ham_spam_to_numeric = StringIndexer(inputCol = 'Spam/Ham', outputCol = 'label')

    clean_up = VectorAssembler(inputCols = ['tf_idf', 'length'], outputCol = 'features')

    pipeline = Pipeline(stages=[ham_spam_to_numeric, tokenizer, stop_remove, count_vec, idf, clean_up])
    cleaner = pipeline.fit(df)
    clean_df = cleaner.transform(df)
    clean_df = clean_df.select('features','label')
    #clean_df.show(3)
    return clean_df