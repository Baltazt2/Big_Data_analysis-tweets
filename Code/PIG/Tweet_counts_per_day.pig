-- Load the input data with correct data types
data = LOAD 'Tweet_sentiment/Tweet_Sentiment_data.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (target:chararray, id:long, date:chararray, 
flag:chararray, user:chararray, tweet:chararray);

--Clean date column, Split the date string into multiple columns
split_date = FOREACH data GENERATE FLATTEN(STRSPLIT(date, ' ')) AS (Day:chararray, Month:chararray, DateNum:int, Time:chararray, TimeZone:chararray, Year:int), id, target, user, tweet;

--Only Keep columns we need, make tweets and users lowercase
cleaned_data = FOREACH split_date GENERATE target, id, Day, Month, Time, Year, LOWER(user) as user, LOWER(tweet) as tweet;

-- Filter rows where the 'target' column equals 4
filtered_data_Pos = FILTER cleaned_data BY target == '4';

grouped_day_data_Pos = group filtered_data_Pos by Day;

result_Pos = FOREACH grouped_day_data_Pos GENERATE group AS Day, COUNT(filtered_data_Pos) AS NumTweets;

-- Filter rows where the 'target' column equals 0
filtered_data_Neg = FILTER cleaned_data BY target == '0';

grouped_day_data_Neg = group filtered_data_Neg by Day;

result_Neg = FOREACH grouped_day_data_Neg GENERATE group AS Day, COUNT(filtered_data_Neg) AS NumTweets;

STORE result_Pos INTO 'Pos_tweets_ByDay' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

STORE result_Neg INTO 'Neg_tweets_ByDay' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
