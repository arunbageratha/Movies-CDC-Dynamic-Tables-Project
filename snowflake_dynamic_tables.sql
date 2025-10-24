-- =====================================================
-- Movies CDC Dynamic Tables Project - Simplified Architecture
-- =====================================================
-- This project demonstrates Change Data Capture (CDC) using Snowflake Streams
-- and Dynamic Tables for real-time movie booking analytics
-- 
-- Architecture:
-- 1. Raw Table: Stores movie booking data
-- 2. Stream: Captures changes to raw table
-- 3. Silver Dynamic Table: Consumes directly from stream (no intermediate CDC table)
-- 4. Gold Dynamic Table: Aggregates analytics from silver layer
-- =====================================================

CREATE DATABASE movies;

USE movies;

CREATE OR REPLACE TABLE raw_movie_bookings (
    booking_id STRING,
    customer_id STRING,
    movie_id STRING,
    booking_date TIMESTAMP,
    status STRING, -- "BOOKED", "CANCELLED"
    ticket_count INT,
    ticket_price NUMBER(10, 2),
    total_amount NUMBER(10, 2) AS (ticket_count * ticket_price), -- Add computed total
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM movie_bookings_stream
ON TABLE raw_movie_bookings;

-- =====================================================
-- TIMESTAMP TRACKING
-- =====================================================
-- created_at: Automatically set when record is inserted
-- updated_at: Can be manually updated when needed
-- =====================================================

-- =====================================================
-- CDC EVENTS TABLE (Raw stream data)
-- =====================================================
-- This table stores raw CDC events from the stream
CREATE OR REPLACE TABLE movie_booking_cdc_events (
    booking_id STRING,
    customer_id STRING,
    movie_id STRING,
    booking_date TIMESTAMP,
    status STRING,
    ticket_count INT,
    ticket_price NUMBER(10, 2),
    total_amount NUMBER(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    change_action STRING,  -- INSERT, UPDATE, DELETE
    is_update BOOLEAN,
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- Insert New Movie Bookings (updated to 2025)
INSERT INTO raw_movie_bookings (booking_id, customer_id, movie_id, booking_date, status, ticket_count, ticket_price)
VALUES
    ('B001', 'C001', 'M001', '2025-09-03 10:00:00', 'BOOKED', 2, 15.00),
    ('B002', 'C002', 'M002', '2025-09-03 10:10:00', 'BOOKED', 1, 12.00),
    ('B003', 'C003', 'M003', '2025-09-03 10:15:00', 'BOOKED', 3, 20.00),
    ('B004', 'C004', 'M004', '2025-09-03 10:20:00', 'BOOKED', 4, 25.00),
    ('B005', 'C005', 'M005', '2025-09-03 10:25:00', 'BOOKED', 1, 10.00);

-- =====================================================
-- TASK TO CONSUME STREAM AND POPULATE CDC EVENTS
-- =====================================================
-- This task runs every minute to consume stream and populate CDC events
CREATE OR REPLACE TASK consume_stream_task
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '1 MINUTE'
AS
INSERT INTO movie_booking_cdc_events (
    booking_id, customer_id, movie_id, booking_date, status, ticket_count, ticket_price, 
    total_amount, created_at, updated_at, change_action, is_update, change_timestamp
)
SELECT 
    booking_id,
    customer_id,
    movie_id,
    booking_date,
    status,
    ticket_count,
    ticket_price,
    total_amount,
    created_at,
    updated_at,
    METADATA$ACTION AS change_action,
    METADATA$ISUPDATE AS is_update,
    CURRENT_TIMESTAMP() AS change_timestamp
FROM movie_bookings_stream
WHERE METADATA$ACTION IS NOT NULL;

-- Start the stream consumption task
ALTER TASK consume_stream_task RESUME;

-- =====================================================
-- SILVER LAYER: Dynamic table with derived fields and filtering
-- =====================================================
-- This dynamic table consumes from CDC events with enhanced derived fields
-- Automatically refreshes every 2 minutes to get latest changes
CREATE OR REPLACE DYNAMIC TABLE movie_bookings_filtered
WAREHOUSE = 'COMPUTE_WH'
TARGET_LAG = DOWNSTREAM
AS
SELECT
    booking_id,
    customer_id,
    movie_id,
    booking_date,
    status,
    ticket_count,
    ticket_price,
    total_amount,
    created_at,
    updated_at,
    change_action,
    is_update,
    change_timestamp,
    -- Derived fields for enhanced analytics
    CASE 
        WHEN status = 'BOOKED' THEN 'ACTIVE'
        WHEN status = 'CANCELLED' THEN 'INACTIVE'
        ELSE 'UNKNOWN'
    END AS booking_status_category,
    
    CASE 
        WHEN ticket_count = 1 THEN 'SINGLE'
        WHEN ticket_count BETWEEN 2 AND 4 THEN 'GROUP'
        WHEN ticket_count >= 5 THEN 'LARGE_GROUP'
        ELSE 'UNKNOWN'
    END AS booking_size_category,
    
    CASE 
        WHEN ticket_price < 10 THEN 'BUDGET'
        WHEN ticket_price BETWEEN 10 AND 20 THEN 'STANDARD'
        WHEN ticket_price > 20 THEN 'PREMIUM'
        ELSE 'UNKNOWN'
    END AS price_category,
    
    -- Time-based derived fields
    DATE(booking_date) AS booking_date_only,
    HOUR(booking_date) AS booking_hour,
    DAYOFWEEK(booking_date) AS day_of_week,
    
    -- Change tracking fields (using existing change_action and is_update)
    
    -- Business logic derived fields
    CASE 
        WHEN status = 'BOOKED' THEN total_amount
        ELSE 0
    END AS active_revenue,
    
    CASE 
        WHEN status = 'CANCELLED' THEN total_amount
        ELSE 0
    END AS lost_revenue,
    
    -- Data quality flags
    CASE 
        WHEN booking_id IS NULL OR customer_id IS NULL OR movie_id IS NULL THEN FALSE
        WHEN ticket_count <= 0 OR ticket_price <= 0 THEN FALSE
        WHEN booking_date > CURRENT_TIMESTAMP() THEN FALSE
        ELSE TRUE
    END AS is_valid_booking

FROM movie_booking_cdc_events
-- Filter out invalid or test data
WHERE booking_id IS NOT NULL 
  AND customer_id IS NOT NULL 
  AND movie_id IS NOT NULL
  AND ticket_count > 0 
  AND ticket_price > 0;

-- =====================================================
-- GOLD LAYER: Enhanced Analytics Dynamic Table
-- =====================================================
-- This dynamic table provides comprehensive aggregated analytics
-- Uses derived fields from the filtered table for richer insights
CREATE OR REPLACE DYNAMIC TABLE movie_booking_insights
WAREHOUSE = 'COMPUTE_WH'
TARGET_LAG = DOWNSTREAM
AS
SELECT
    movie_id,
    
    -- Basic metrics
    COUNT(booking_id) AS total_bookings,
    COUNT(CASE WHEN is_valid_booking = TRUE THEN 1 END) AS valid_bookings,
    COUNT(CASE WHEN is_valid_booking = FALSE THEN 1 END) AS invalid_bookings,
    
    -- Revenue metrics using derived fields
    SUM(active_revenue) AS total_active_revenue,
    SUM(lost_revenue) AS total_lost_revenue,
    SUM(total_amount) AS total_gross_revenue,
    
    -- Booking status breakdown
    COUNT(CASE WHEN booking_status_category = 'ACTIVE' THEN 1 END) AS active_bookings,
    COUNT(CASE WHEN booking_status_category = 'INACTIVE' THEN 1 END) AS cancelled_bookings,
    
    -- Booking size analysis
    COUNT(CASE WHEN booking_size_category = 'SINGLE' THEN 1 END) AS single_bookings,
    COUNT(CASE WHEN booking_size_category = 'GROUP' THEN 1 END) AS group_bookings,
    COUNT(CASE WHEN booking_size_category = 'LARGE_GROUP' THEN 1 END) AS large_group_bookings,
    
    -- Price category analysis
    COUNT(CASE WHEN price_category = 'BUDGET' THEN 1 END) AS budget_bookings,
    COUNT(CASE WHEN price_category = 'STANDARD' THEN 1 END) AS standard_bookings,
    COUNT(CASE WHEN price_category = 'PREMIUM' THEN 1 END) AS premium_bookings,
    
    -- Change tracking metrics
    COUNT(CASE WHEN change_action = 'INSERT' THEN 1 END) AS new_bookings,
    COUNT(CASE WHEN change_action = 'UPDATE' AND is_update = TRUE THEN 1 END) AS status_changes,
    COUNT(CASE WHEN change_action = 'DELETE' THEN 1 END) AS deleted_bookings,
    
    -- Calculated metrics
    ROUND(COUNT(CASE WHEN booking_status_category = 'INACTIVE' THEN 1 END) * 100.0 / COUNT(booking_id), 2) AS cancellation_rate_percent,
    ROUND(COUNT(CASE WHEN booking_status_category = 'ACTIVE' THEN 1 END) * 100.0 / COUNT(booking_id), 2) AS active_rate_percent,
    ROUND(SUM(active_revenue) / NULLIF(COUNT(CASE WHEN booking_status_category = 'ACTIVE' THEN 1 END), 0), 2) AS avg_revenue_per_active_booking,
    
    -- Time-based metrics
    COUNT(DISTINCT booking_date_only) AS booking_days,
    COUNT(DISTINCT booking_hour) AS active_hours,
    
    -- Data quality metrics
    ROUND(COUNT(CASE WHEN is_valid_booking = TRUE THEN 1 END) * 100.0 / COUNT(booking_id), 2) AS data_quality_score,
    
    CURRENT_TIMESTAMP() AS refresh_timestamp

FROM movie_bookings_filtered
WHERE is_valid_booking = TRUE  -- Only include valid bookings in analytics
GROUP BY movie_id;


-- Optional refresh task
CREATE OR REPLACE TASK refresh_movie_booking_insights
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '1 MINUTE'
AS
ALTER DYNAMIC TABLE movie_booking_insights REFRESH;

ALTER TASK refresh_movie_booking_insights RESUME;

-- Update Booking Status to CANCELLED
UPDATE raw_movie_bookings
SET status = 'CANCELLED', updated_at = CURRENT_TIMESTAMP()
WHERE booking_id IN ('B001', 'B003');


-- Dynamic table refresh history
SELECT 'DYNAMIC TABLE REFRESH HISTORY' AS info;
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(TABLE_NAME=>'movie_bookings_filtered')) ORDER BY REFRESH_START_TIME DESC LIMIT 5;

