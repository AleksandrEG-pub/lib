ALTER TABLE web_logs DROP PROJECTION IF EXISTS day_projection;
Alter table web_logs
add projection day_projection 
(
  SELECT
      toDate(timestamp) AS date,
      count(url) AS request_count
  GROUP BY date
);
Alter table web_logs materialize projection day_projection;


ALTER TABLE web_logs DROP PROJECTION IF EXISTS avg_response_projection;
Alter table web_logs
add projection avg_response_projection 
(
    SELECT url, avg(response_time) AS avg_response
    GROUP BY url
);
Alter table web_logs materialize projection avg_response_projection;


ALTER TABLE web_logs DROP PROJECTION IF EXISTS user_requests_count_projection;
Alter table web_logs
add projection user_requests_count_projection
(
    SELECT user_id, count(url)
    GROUP BY user_id
);
Alter table web_logs materialize projection user_requests_count_projection;
