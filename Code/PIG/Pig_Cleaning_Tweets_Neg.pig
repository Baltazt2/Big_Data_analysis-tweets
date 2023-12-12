-- Load the input data with correct data types
data = LOAD 'Tweet_sentiment/Tweet_Sentiment_data.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (target:chararray, id:long, date:chararray, 
flag:chararray, user:chararray, tweet:chararray);

--Clean date column

cleaned_data = FOREACH data GENERATE target, id, date, user, LOWER(tweet) as tweet;


-- Filter rows where the 'target' column equals 0 (Negative tweets)
filtered_data = FILTER cleaned_data BY target == '0';


-- Load stop words
stopwords = LOAD 'stopWords.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (stopword:chararray);

-- Tokenize the tweet column
tokenized_data = FOREACH filtered_data GENERATE FLATTEN(TOKENIZE(tweet)) AS word;

-- Filter out stop words
joined_data = join tokenized_data by word LEFT OUTER, stopwords by stopword USING 'replicated';

-- Group by id to reconstruct the cleaned tweet
filtered_joined = filter joined_data by stopword IS NULL;

-- Concatenate the words to reconstruct the cleaned tweet
filtered_joined = foreach filtered_joined generate word;

grouped_data = group filtered_joined by word;

word_counts = foreach grouped_data generate COUNT(filtered_joined) as count, group as word;

ordered_counts = order word_counts by count desc; 

limited_counts = LIMIT ordered_counts 10000;
limited_counts_with_schema = FOREACH limited_counts GENERATE $0 as count:int, $1 as word:chararray;

-- Store Negative word counts
STORE limited_counts_with_schema INTO 'Neg_word_counts2.csv' USING PigStorage(',');

