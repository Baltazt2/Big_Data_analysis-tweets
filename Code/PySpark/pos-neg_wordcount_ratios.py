from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("join_word_counts").getOrCreate()

# Read the first CSV file into a DataFrame and alias columns
df1 = spark.read.csv("Neg_counts.csv", header=False, inferSchema=True)
df1 = df1.withColumnRenamed("_c0", "count_neg").withColumnRenamed("_c1", "word")

# Read the second CSV file into another DataFrame and alias columns
df2 = spark.read.csv("Pos_counts.csv", header=False, inferSchema=True)
df2 = df2.withColumnRenamed("_c0", "count_pos").withColumnRenamed("_c1", "word")

# Perform the join on the 'word' column
merged_df = df1.join(df2, on='word', how='inner')

# Add a new column 'most_neg' with the values in count_neg divided by count_pos
merged_df = merged_df.withColumn("most_neg", col("count_neg") / col("count_pos"))

# Filter the DataFrame to include only rows where count_neg > 1000
merged_df = merged_df.filter(col("count_neg") > 1000)

# Order the DataFrame by 'most_neg' in descending order
merged_df = merged_df.orderBy("most_neg", ascending=False)

# Display the combined DataFrame
merged_df.show()

# Write the merged DataFrame to a new CSV file
merged_df.write.csv("output_most_neg/most_neg.csv", header=False, mode="overwrite")

# Stop the Spark session
spark.stop()





from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("join_word_counts").getOrCreate()

# Read the first CSV file into a DataFrame and alias columns
df1 = spark.read.csv("Neg_counts.csv", header=False, inferSchema=True)
df1 = df1.withColumnRenamed("_c0", "count_neg").withColumnRenamed("_c1", "word")

# Read the second CSV file into another DataFrame and alias columns
df2 = spark.read.csv("Pos_counts.csv", header=False, inferSchema=True)
df2 = df2.withColumnRenamed("_c0", "count_pos").withColumnRenamed("_c1", "word")

# Perform the join on the 'word' column
merged_df = df1.join(df2, on='word', how='inner')

# Add a new column 'most_neg' with the values in count_neg divided by count_pos
merged_df = merged_df.withColumn("most_pos", col("count_pos") / col("count_neg"))

# Filter the DataFrame to include only rows where count_pos > 1000
merged_df = merged_df.filter(col("count_pos") > 1000)

# Order the DataFrame by 'most_pos' in descending order
merged_df = merged_df.orderBy("most_pos", ascending=False)

# Display the combined DataFrame
merged_df.show()

# Write the merged DataFrame to a new CSV file
merged_df.write.csv("output_most_pos/most_pos.csv", header=False, mode="overwrite")

# Stop the Spark session
spark.stop()
