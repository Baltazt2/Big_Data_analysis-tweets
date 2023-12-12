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

# List to store accuracy values
accuracy_values = []

# Iterate over different smoothing parameters
for smoothing_param in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
    # Train a Naive Bayes model
    nb = NaiveBayes(featuresCol="features", labelCol="label", smoothing=smoothing_param, modelType="multinomial")
    nb_model = nb.fit(train_data)

    # Transform the test data
    test_data_with_predictions = nb_model.transform(test_data)

    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(test_data_with_predictions)

    # Store the accuracy value in the list
    accuracy_values.append(accuracy)

# Print the accuracy values at the end
for smoothing_param, accuracy in zip([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], accuracy_values):
    print("Smoothing = {:.2f}: Accuracy = {:.2f}%".format(smoothing_param, accuracy * 100))

# Stop the Spark session
spark.stop()