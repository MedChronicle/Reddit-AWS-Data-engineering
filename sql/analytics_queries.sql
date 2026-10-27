-- ===================== Postgres (schema "reddit") =====================
-- Today's ten best posts per day
SELECT post_date, day_rank, score, num_comments, title FROM reddit.v_daily_top_posts ORDER BY post_date DESC, day_rank;

-- Who earns the most karma in the subreddit?
SELECT * FROM reddit.v_author_leaderboard LIMIT 20;

-- Which topics are trending this week?
SELECT primary_tag, SUM(posts) AS posts, ROUND(AVG(avg_score), 1) AS avg_score
FROM reddit.v_tag_trends WHERE post_date >= to_char(now() - interval '7 days', 'YYYY-MM-DD')
GROUP BY primary_tag ORDER BY posts DESC;

-- Posts that are still gaining score on later days (needs >1 daily run)
SELECT p.title, v.snapshot_date, v.score_change, v.comments_change
FROM reddit.v_score_velocity v JOIN reddit.posts p USING (id)
WHERE v.score_change > 0 ORDER BY v.score_change DESC LIMIT 20;

-- Best hour (UTC) to post: average score by hour
SELECT post_hour_utc, COUNT(*) AS posts, ROUND(AVG(score)::numeric, 1) AS avg_score
FROM reddit.posts GROUP BY 1 ORDER BY avg_score DESC;

-- Do questions get more comments than other posts?
SELECT is_question, COUNT(*) AS posts, ROUND(AVG(num_comments)::numeric, 1) AS avg_comments
FROM reddit.posts GROUP BY 1;

-- ===================== Athena =====================
-- SELECT primary_tag, count(*) AS posts, avg(score) AS avg_score
-- FROM reddit.posts_processed WHERE subreddit = 'dataengineering' AND dt >= '2024-10-01'
-- GROUP BY primary_tag ORDER BY posts DESC;
