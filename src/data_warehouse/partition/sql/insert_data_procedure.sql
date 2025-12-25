-- date format YYYY-MM-DD
-- example:
-- generate_orders('2022-01-01', 1000000, 3)
CREATE OR REPLACE PROCEDURE generate_orders(begin_date_str varchar, total_orders integer, period_years integer)
LANGUAGE plpgsql
AS $$
DECLARE
    begin_date date = begin_date_str::date;
    days_year integer= 365;
    range_days integer = period_years * days_year; 
BEGIN
    INSERT INTO orders (price, load_date)
    SELECT 
        round((10.00 + random() * 9990.00)::numeric, 2) as price,
        (
            begin_date + 
            (floor(((n - 1) * 1.0 / total_orders) * range_days) * interval '1 day')::interval
        ) as load_date
    FROM generate_series(1, total_orders) n;
END;
$$;
