INSERT INTO web_logs (timestamp, user_id, url, response_time, status_code)
SELECT 
    now() - randUniform(0, 3600 * 24 * 30 * 6) as timestamp,  
    rand() % 10000 + 1 as user_id,               
    ['/home', '/products', '/cart', '/checkout', 
    '/login', '/api/data', '/images/1.jpg', 
    '/profile', '/search?q=clickhouse'][rand() % 9 + 1] as url,
    randLogNormal(5, 1.5) as response_time,    
    [200, 200, 200, 200, 404, 500, 302, 301][rand() % 8 + 1] as status_code
FROM numbers(10000000);