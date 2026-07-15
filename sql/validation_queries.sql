SELECT COUNT(*) AS daily_revenue_rows
FROM analytics.daily_revenue;

SELECT pickup_date, trip_count, total_revenue
FROM analytics.daily_revenue
ORDER BY pickup_date
LIMIT 10;

SELECT pickup_borough, pickup_zone, trip_count, total_revenue
FROM analytics.pickup_zone_summary
ORDER BY total_revenue DESC
LIMIT 10;

SELECT payment_method, trip_count, total_revenue
FROM analytics.payment_method_summary
ORDER BY trip_count DESC;
