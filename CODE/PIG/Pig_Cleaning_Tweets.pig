-- Load the input data with correct data types
data = LOAD 'Tweet_sentiment/Tweet_Sentiment_data.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (target:chararray, id:long, date:chararray, 
flag:chararray, user:chararray, tweet:chararray);

--Clean date column, Split the date string into multiple columns
split_date = FOREACH data GENERATE FLATTEN(STRSPLIT(date, ' ')) AS (Day:chararray, Month:chararray, DateNum:int, Time:chararray, TimeZone:chararray, Year:int), id, target, user, tweet;

--Only Keep columns we need, make tweets and users lowercase
cleaned_data = FOREACH split_date GENERATE target, id, Day, Month, Time, Year, LOWER(user) as user, LOWER(tweet) as tweet;

STORE cleaned_data INTO 'cleaned_tweet_dataset' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
