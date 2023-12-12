from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import lower, col

# Initialize Spark Session
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

# Load the dataset without specifying headers
data = spark.read.csv("Tweet_Sentiment_data.csv", header=False, inferSchema=True)

# Assuming the tweet text is in the fifth column (index 5)
data = data.withColumnRenamed("_c5", "text")
data = data.withColumnRenamed("_c0", "target")

columns_to_drop = ["_c1", "_c2", "_c3", "_c4"]
data = data.drop(*columns_to_drop)

# Assuming 'text' is the column containing tweets
data = data.withColumn("text", lower(col("text")))

# Tokenize the data
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashing_tf = HashingTF(inputCol="words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")

# Convert the tokenized words into a feature vector using TF-IDF
label_indexer = StringIndexer(inputCol="target", outputCol="label")

# Create a pipeline
pipeline = Pipeline(stages=[tokenizer, hashing_tf, idf, label_indexer])

# Split the data
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Fit the pipeline on the training data
model = pipeline.fit(train_data)

# Transform both training and test data
train_data = model.transform(train_data)
test_data = model.transform(test_data)

# Train a Naive Bayes model
nb = NaiveBayes(featuresCol="features", labelCol="label", smoothing=1.0, modelType="multinomial")
model = nb.fit(train_data)

# Transform the test data
test_data = model.transform(test_data)

# Evaluate the model
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(test_data)
print("Accuracy: {:.2f}%".format(accuracy * 100))


# Save the Naive Bayes model
nb_model_path = "naive_bayes_model"
model.save(nb_model_path)

# Inform the user about the exported Naive Bayes model path
print(f"Naive Bayes model saved to: {nb_model_path}")