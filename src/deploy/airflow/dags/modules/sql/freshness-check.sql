insert into data_quality_checks(check_name, status, value)
select 
    'freshness' as check_name,
    CASE 
        WHEN (current_timestamp - re.loaded_at) < interval '24 hour' 
        THEN 'OK'
        ELSE 'NOK'
    end as check_status, 
    (current_timestamp - loaded_at) as value
from raw_employees re 
order by loaded_at desc 
limit 1;