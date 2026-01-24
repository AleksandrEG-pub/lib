CREATE TABLE IF NOT EXISTS flights (
    flight_id BIGSERIAL PRIMARY KEY,
    airline VARCHAR(100),
    flight_number VARCHAR(20),
    origin VARCHAR(3),
    destination VARCHAR(3),
    departure_time timestamp with time zone,
    arrival_time timestamp with time zone,
    duration_minutes INTEGER,
    aircraft_type VARCHAR(50),
    status VARCHAR(20),
    economy_seats INTEGER DEFAULT 0,
    business_seats INTEGER DEFAULT 0,
    first_class_seats INTEGER DEFAULT 0,
    booked_economy INTEGER DEFAULT 0,
    booked_business INTEGER DEFAULT 0,
    booked_first_class INTEGER DEFAULT 0
);