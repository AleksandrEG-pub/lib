-- ▪ Найдите общее количество юзеров за час за последнюю неделю.
select toStartOfDay(timestamp), count(url)
from web_logs
where timestamp > addDays(now(), -7)
GROUP by toStartOfDay(timestamp)
order by toStartOfDay(timestamp);

-- ▪ Найдите общее количество запросов за каждый день.
SELECT 
  toDate(timestamp) AS date,
  count(url) AS request_count
FROM web_logs wl
GROUP BY toDate(wl.timestamp);

-- ▪️ Определите среднее время ответа для каждого URL. 
SELECT url, avg(response_time) AS avg_response_ms
FROM web_logs wl 
GROUP BY url;

-- ▪️ Подсчитайте количество запросов с ошибками (например, статус-коды 4xx и 5xx).
SELECT *
FROM error_counts_mv ;

-- Найдите топ-10 пользователей по количеству запросов.
explain
SELECT user_id, count(url)
FROM web_logs wl 
GROUP BY user_id 
ORDER BY count(url) DESC
LIMIT 10;

