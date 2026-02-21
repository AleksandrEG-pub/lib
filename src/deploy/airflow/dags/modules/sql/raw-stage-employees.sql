INSERT INTO stage_employees (
    hash_key,
    employee_id,
    name,
    age,
    city,
    salary,
    loaded_at,
    record_source
)
SELECT 
    MD5(CONCAT(
        COALESCE(name, ''), 
        '|',
        COALESCE(age, ''),
        '|',
        COALESCE(city, ''),
        '|',
        COALESCE(salary, '')
    )) AS hash_key,
    employee_id,
    TRIM(name) AS name,
    CASE 
        WHEN age IS NULL OR age = '' OR age ~ '[^0-9]' THEN NULL
        ELSE CAST(age AS INTEGER)
    END AS age,
    TRIM(city) AS city,
    CASE 
        WHEN salary IS NULL OR salary = '' OR salary ~ '[^0-9\.]' THEN NULL
        ELSE CAST(salary AS DECIMAL(10,2))
    END AS salary,
    loaded_at,
    record_source
FROM raw_employees
ON CONFLICT DO NOTHING;
