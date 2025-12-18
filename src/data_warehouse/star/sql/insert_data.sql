insert into product_dim(product_name, category, start_date, end_date, is_current)
values
('Palm Jumeirah Villa - Signature Series', 'Premium Apartments', '2023-01-01', '9999-12-31', true),
('Burj Khalifa Sky Residence', 'Ultra-Luxury Apartments', '2023-01-01', '9999-12-31', true),
('Emirates Hills Mansion', 'Premium Villas', '2023-01-01', '2024-12-31', true),
('Bluewaters Island Penthouse', 'Luxury Waterfront', '2023-06-01', '9999-12-31', true),
('Jumeirah Bay Island Residence', 'Premium Beachfront', '2022-03-15', '2023-12-31', true);

insert into customer_dim (name, email, phone, start_date, end_date, is_current)
values 
('Bruce Banner', 'bruce.banner@gamma-lab.com', '+971506666006', '2020-08-10', '9999-12-31', true),
('Natasha Romanoff', 'blackwidow@shield.gov', '+971508888008', '2019-04-15', '9999-12-31', true),
('Peter Parker', 'peterp@midtownhigh.edu', '+971509999009', '2021-02-28', '9999-12-31', true);

insert into date_dim (date_value)
values 
('2023-06-15 10:30:00'),
('2023-11-20 14:45:00'),
('2023-09-05 9:15:00'),
('2024-01-10 11:00:00');

insert into sales_fact (customer_id, product_id, date_id, amount, quantity)
values 
-- Bruce Banner buys a Burj Khalifa residence on 2023-11-20
(1, 2, 2, 55000000, 1),
-- Natasha Romanoff buys a Bluewaters Penthouse on 2023-06-15
(2, 4, 1, 22500000, 1),
-- Peter Parker buys a Palm Jumeirah Villa on 2023-09-05
(3, 1, 3, 15000000, 1),
-- Bruce Banner makes a purchase on 2024-01-10
(1, 3, 4, 30000000, 1);

