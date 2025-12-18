-- update phone 
update customer_dim 
set end_date = '2023-05-31', is_current = false
where name = 'Bruce Banner' and email = 'bruce.banner@gamma-lab.com' 
and phone = '+971506666006' and is_current = true;

insert into customer_dim (name, email, phone, start_date, end_date, is_current)
values 
('Bruce Banner', 'bruce.banner@gamma-lab.com', '+971507777007', '2023-06-01', '9999-12-31', true);


-- update email
update customer_dim 
set end_date = '2023-11-14', is_current = false
where name = 'Natasha Romanoff' and email = 'blackwidow@shield.gov' 
and phone = '+971508888008' and is_current = true;

insert into customer_dim (name, email, phone, start_date, end_date, is_current)
values 
('Natasha Romanoff', 'natasha@avengerscompound.org', '+971508888008', '2023-11-15', '9999-12-31', true);


-- update email and phone
update customer_dim 
set end_date = '2023-08-31', is_current = false
where name = 'Peter Parker' and email = 'peterp@midtownhigh.edu' 
and phone = '+971509999009' and is_current = true;

insert into customer_dim (name, email, phone, start_date, end_date, is_current)
values 
('Peter Parker', 'p.parker@dailybugle.ae', '+971500000010', '2023-09-01', '9999-12-31', true);