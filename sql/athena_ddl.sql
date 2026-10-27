-- Athena table over the Parquet files written to s3://<bucket>/processed/
-- Generated from the real Parquet schema. `subreddit` is a PARTITION column here (it is also
-- stored inside the files, but is deliberately not declared as a normal column). Not yet run against a live
-- Athena - review once before relying on it.
CREATE DATABASE IF NOT EXISTS reddit;

CREATE EXTERNAL TABLE IF NOT EXISTS reddit.posts_processed (
  `id` string,
  `title` string,
  `score` bigint,
  `num_comments` bigint,
  `author` string,
  `created_utc` timestamp,
  `url` string,
  `over_18` boolean,
  `edited` boolean,
  `spoiler` boolean,
  `stickied` boolean,
  `upvote_ratio` double,
  `link_flair_text` string,
  `domain` string,
  `is_self` boolean,
  `permalink` string,
  `total_awards_received` bigint,
  `num_crossposts` bigint,
  `selftext` string,
  `edited_at` timestamp,
  `extracted_at` timestamp,
  `post_date` string,
  `post_hour_utc` bigint,
  `post_weekday` string,
  `title_length` bigint,
  `title_word_count` bigint,
  `is_question` boolean,
  `has_flair` boolean,
  `age_hours` double,
  `engagement_ratio` double,
  `comments_per_hour` double,
  `hotness` double,
  `url_domain` string,
  `tags` string,
  `primary_tag` string
)
PARTITIONED BY (subreddit string, dt string)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/processed/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Run after every load (or use partition projection / a Glue crawler) to register new partitions
MSCK REPAIR TABLE reddit.posts_processed;
