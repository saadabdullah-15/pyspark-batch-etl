CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.daily_revenue (
    pickup_date DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    trip_count INTEGER NOT NULL,
    total_revenue NUMERIC(14, 2) NOT NULL,
    average_trip_revenue NUMERIC(14, 2) NOT NULL,
    average_trip_distance NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.pickup_zone_summary (
    pickup_borough TEXT NOT NULL,
    pickup_zone TEXT NOT NULL,
    trip_count INTEGER NOT NULL,
    total_revenue NUMERIC(14, 2) NOT NULL,
    average_tip_amount NUMERIC(10, 2) NOT NULL,
    unique_dropoff_zones INTEGER NOT NULL,
    PRIMARY KEY (pickup_borough, pickup_zone)
);

CREATE TABLE IF NOT EXISTS analytics.payment_method_summary (
    payment_method TEXT PRIMARY KEY,
    trip_count INTEGER NOT NULL,
    total_revenue NUMERIC(14, 2) NOT NULL,
    average_tip_amount NUMERIC(10, 2) NOT NULL
);
