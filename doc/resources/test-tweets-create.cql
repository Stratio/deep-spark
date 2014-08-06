-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS test 
WITH REPLICATION = {
       'class' : 'SimpleStrategy', 
       'replication_factor': 1
};

-- Create table
use test;

CREATE TABLE IF NOT EXISTS tweets(
       "tweet_id" uuid PRIMARY KEY,
       "tweet_date" timestamp,
       author text,
       hashtags set<text>,
       "favorite_count" int,
       content text,
       truncated boolean);
