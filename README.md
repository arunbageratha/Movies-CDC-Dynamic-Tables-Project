# 🎬 Movies CDC Dynamic Tables Project

## Overview

This project demonstrates a comprehensive **Change Data Capture (CDC)** solution using Snowflake's advanced features including **Streams**, **Tasks**, and **Dynamic Tables**. The system provides real-time movie booking analytics with enhanced derived fields, automated data processing, and an interactive Streamlit dashboard.

## 🏗️ Architecture

### Core Components

1. **Source Table**: `raw_movie_bookings` - Stores raw booking transactions with timestamps
2. **Stream**: `movie_bookings_stream` - Captures all changes to source table
3. **CDC Events Table**: `movie_booking_cdc_events` - Stores raw stream data with metadata
4. **Dynamic Tables**: 
   - `movie_bookings_filtered` - Enhanced view with derived fields and business logic
   - `movie_booking_insights` - Comprehensive analytics with business categorizations
5. **Tasks**: Automated processing of CDC events (every 1 minute)
6. **Streamlit Dashboard**: Clean, beginner-friendly analytics interface

### Enhanced Data Flow

```
Raw Bookings → Stream → CDC Events → Enhanced Filtered Table → Analytics Dashboard
     ↓              ↓         ↓              ↓                    ↓
  INSERT/      Captures   Raw Stream    Derived Fields        Interactive
 UPDATE/       Changes    Data +        + Business Logic      Visualization
DELETE                     Metadata     + Data Quality
```

### Key Enhancements

- **Derived Fields**: Business categorizations (ACTIVE/INACTIVE, SINGLE/GROUP, BUDGET/PREMIUM)
- **Data Quality**: Built-in validation and quality scoring
- **Enhanced Analytics**: Rich metrics with business context
- **Simplified Dashboard**: Clean interface focused on essential features

## 📊 Key Features

### Real-time CDC Processing
- **Automatic Change Detection**: Streams capture all INSERT, UPDATE, DELETE operations
- **Raw Stream Data**: Complete change history with metadata
- **Near Real-time Processing**: Tasks run every minute, dynamic tables refresh every 2 minutes
- **Timestamp Tracking**: Automatic created_at and updated_at management

### Enhanced Analytics with Derived Fields
- **Business Categorizations**: 
  - Status Categories: ACTIVE (BOOKED) vs INACTIVE (CANCELLED)
  - Size Categories: SINGLE, GROUP, LARGE_GROUP based on ticket count
  - Price Categories: BUDGET, STANDARD, PREMIUM based on ticket price
- **Revenue Analysis**: Active revenue vs lost revenue tracking
- **Data Quality Metrics**: Built-in validation and quality scoring
- **Time-based Analysis**: Booking hour, day of week patterns

### Simplified Dashboard
- **Essential Filters**: Date range, booking status, movie selection
- **Key Metrics**: Total bookings, revenue, active/lost revenue
- **Core Visualizations**: Revenue by status, booking distribution, movie performance
- **Clean Interface**: Beginner-friendly design with focused functionality
- **Export Capabilities**: Download filtered data as CSV

## 🚀 Getting Started

### Prerequisites

- Snowflake account with appropriate privileges
- Access to `COMPUTE_WH` warehouse
- Streamlit environment (for dashboard)

### Setup Instructions

1. **Execute SQL Script**:
   ```sql
   -- Run the complete snowflake_dynamic_tables.sql script
   -- This will create all tables, streams, tasks, and dynamic tables
   ```

2. **Verify Setup**:
   ```sql
   -- Check that all objects are created successfully
   SHOW TABLES;
   SHOW STREAMS;
   SHOW TASKS;
   SHOW DYNAMIC TABLES;
   ```

3. **Run Streamlit Dashboard**:
   ```bash
   streamlit run streamlit_app.py
   ```

### Sample Data

The project includes realistic sample data with:
- **5 initial bookings** across 5 different movies (September 2025)
- **Booking statuses**: BOOKED and CANCELLED (simplified for clarity)
- **Various ticket prices** ($10-$25) and quantities (1-4 tickets)
- **Time-stamped transactions** with automatic created_at/updated_at tracking
- **Realistic movie data**: Popular movies with different price points

## 📋 Database Schema

### Source Table: `raw_movie_bookings`
```sql
CREATE TABLE raw_movie_bookings (
    booking_id STRING,                    -- Unique booking identifier
    customer_id STRING,                   -- Customer identifier  
    movie_id STRING,                      -- Movie identifier
    booking_date TIMESTAMP,               -- When booking was made
    status STRING,                        -- BOOKED, CANCELLED (simplified)
    ticket_count INT,                     -- Number of tickets
    ticket_price NUMBER(10, 2),           -- Price per ticket
    total_amount NUMBER(10, 2) AS (ticket_count * ticket_price), -- Computed total
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### CDC Events Table: `movie_booking_cdc_events`
```sql
CREATE TABLE movie_booking_cdc_events (
    -- All original booking fields
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
    -- CDC metadata
    change_action STRING,                 -- INSERT, UPDATE, DELETE
    is_update BOOLEAN,                    -- TRUE for updates
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### Enhanced Filtered Table: `movie_bookings_filtered`
```sql
-- Dynamic table with derived fields and business logic
CREATE DYNAMIC TABLE movie_bookings_filtered AS
SELECT
    -- Original fields
    booking_id, customer_id, movie_id, booking_date, status,
    ticket_count, ticket_price, total_amount, created_at, updated_at,
    change_action, is_update, change_timestamp,
    
    -- Derived business fields
    CASE 
        WHEN status = 'BOOKED' THEN 'ACTIVE'
        WHEN status = 'CANCELLED' THEN 'INACTIVE'
    END AS booking_status_category,
    
    CASE 
        WHEN ticket_count = 1 THEN 'SINGLE'
        WHEN ticket_count BETWEEN 2 AND 4 THEN 'GROUP'
        WHEN ticket_count >= 5 THEN 'LARGE_GROUP'
    END AS booking_size_category,
    
    CASE 
        WHEN ticket_price < 10 THEN 'BUDGET'
        WHEN ticket_price BETWEEN 10 AND 20 THEN 'STANDARD'
        WHEN ticket_price > 20 THEN 'PREMIUM'
    END AS price_category,
    
    -- Revenue analysis
    CASE WHEN status = 'BOOKED' THEN total_amount ELSE 0 END AS active_revenue,
    CASE WHEN status = 'CANCELLED' THEN total_amount ELSE 0 END AS lost_revenue,
    
    -- Data quality
    CASE 
        WHEN booking_id IS NULL OR customer_id IS NULL OR movie_id IS NULL THEN FALSE
        WHEN ticket_count <= 0 OR ticket_price <= 0 THEN FALSE
        ELSE TRUE
    END AS is_valid_booking

FROM movie_booking_cdc_events
WHERE booking_id IS NOT NULL AND customer_id IS NOT NULL;
```

## 🔄 CDC Processing Logic

### Stream Processing
- **Automatic Capture**: Streams automatically detect all changes to source table
- **Metadata Addition**: Adds `METADATA$ACTION` and `METADATA$ISUPDATE` columns
- **Raw Data Storage**: Complete change history preserved in CDC events table

### Task Automation (`consume_stream_task`)
- **Scheduled Execution**: Runs every minute to process new changes
- **Raw Stream Consumption**: Populates CDC events table with complete stream data
- **Metadata Preservation**: Maintains all original fields plus change metadata
- **Error Handling**: Built-in retry and error logging capabilities

### Dynamic Table Processing
- **Enhanced Filtered Table**: 2-minute refresh lag, consumes from CDC events
- **Derived Field Calculation**: Business logic applied during refresh
- **Data Quality Filtering**: Invalid records filtered out automatically
- **Analytics Table**: Downstream refresh, aggregates from filtered table

## 📈 Analytics Capabilities

### Enhanced Key Performance Indicators
- **Total Bookings**: Count of all valid booking transactions
- **Active Revenue**: Revenue from BOOKED status bookings
- **Lost Revenue**: Revenue from CANCELLED status bookings
- **Data Quality Score**: Percentage of valid bookings
- **Cancellation Rate**: Percentage of cancelled bookings

### Business Categorization Analytics
- **Status Categories**: ACTIVE vs INACTIVE booking analysis
- **Size Categories**: SINGLE, GROUP, LARGE_GROUP booking patterns
- **Price Categories**: BUDGET, STANDARD, PREMIUM revenue analysis
- **Change Tracking**: INSERT, UPDATE, DELETE operation metrics

### Movie Performance Metrics
- **Revenue Analysis**: Active revenue vs lost revenue by movie
- **Booking Volume**: Total bookings with validity checks
- **Category Breakdown**: Bookings by size and price categories
- **Change Metrics**: New bookings, status changes, deletions

### Time-based Analysis
- **Date Range Filtering**: Flexible date range selection
- **Booking Hour Analysis**: Peak booking times
- **Day of Week Patterns**: Weekly booking trends
- **Real-time Updates**: 2-minute refresh for current insights

## 🎯 Use Cases

### Business Intelligence
- **Revenue Optimization**: Identify high-performing movies and time slots
- **Customer Behavior**: Analyze booking patterns and preferences
- **Operational Efficiency**: Monitor cancellation rates and booking trends

### Real-time Monitoring
- **Live Dashboard**: Monitor booking activity in real-time
- **Alert Systems**: Set up notifications for unusual patterns
- **Performance Tracking**: Track key metrics as they change

### Data Quality
- **Change Tracking**: Complete audit trail of all data modifications
- **Data Lineage**: Track data flow from source to analytics
- **Compliance**: Maintain historical records for regulatory requirements

## 🔧 Configuration Options

### Task Scheduling
```sql
-- Modify task frequency
ALTER TASK consume_stream_task 
SET SCHEDULE = '30 SECONDS';  -- More frequent processing

-- Suspend/resume tasks
ALTER TASK consume_stream_task SUSPEND;
ALTER TASK consume_stream_task RESUME;
```

### Dynamic Table Settings
```sql
-- Modify filtered table refresh frequency
ALTER DYNAMIC TABLE movie_bookings_filtered 
SET TARGET_LAG = '1 MINUTE';  -- More frequent refresh

-- Manual refresh
ALTER DYNAMIC TABLE movie_bookings_filtered REFRESH;
ALTER DYNAMIC TABLE movie_booking_insights REFRESH;
```

### Warehouse Configuration
```sql
-- Use different warehouse for processing
ALTER TASK consume_stream_task 
SET WAREHOUSE = 'ANALYTICS_WH';
```

## 📊 Dashboard Features

### Essential Interactive Filters
- **Date Range Selection**: Default September 2025, flexible date range
- **Status Filtering**: BOOKED, CANCELLED, or All bookings
- **Movie Selection**: Individual movie analysis or All movies
- **Refresh Button**: Manual data refresh capability

### Core Visualizations
- **Revenue by Status**: Bar chart showing active vs lost revenue
- **Booking Distribution**: Pie chart of booking status breakdown
- **Movie Performance Table**: Detailed metrics by movie
- **Real-time Insights**: Live analytics from dynamic tables

### Export and Navigation
- **CSV Download**: Export filtered data with timestamp
- **Raw Data View**: Expandable section for detailed data inspection
- **Clean Interface**: Beginner-friendly design with essential features
- **Responsive Layout**: Optimized for different screen sizes

## 🚨 Monitoring and Troubleshooting

### Task Monitoring
```sql
-- Check task execution history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'consume_stream_task'
)) ORDER BY SCHEDULED_TIME DESC;

-- Check task status
SHOW TASKS;
```

### Stream and CDC Monitoring
```sql
-- Check stream data
SELECT * FROM movie_bookings_stream;

-- Check CDC events (raw stream data)
SELECT * FROM movie_booking_cdc_events 
ORDER BY change_timestamp DESC 
LIMIT 10;

-- Check filtered data with derived fields
SELECT booking_id, status, booking_status_category, 
       booking_size_category, price_category, active_revenue, lost_revenue
FROM movie_bookings_filtered 
ORDER BY change_timestamp DESC;
```

### Dynamic Table Status
```sql
-- Check dynamic table refresh status
SHOW DYNAMIC TABLES;

-- Check refresh history for filtered table
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    TABLE_NAME => 'movie_bookings_filtered'
)) ORDER BY REFRESH_START_TIME DESC LIMIT 5;

-- Check analytics table refresh
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    TABLE_NAME => 'movie_booking_insights'
)) ORDER BY REFRESH_START_TIME DESC LIMIT 5;
```

## 🔒 Security Considerations

### Access Control
- **Role-based Access**: Use appropriate Snowflake roles
- **Warehouse Permissions**: Ensure proper warehouse access
- **Data Privacy**: Consider PII data handling requirements

### Data Governance
- **Audit Trail**: Complete change history maintained
- **Data Retention**: Configure appropriate retention policies
- **Backup Strategy**: Implement regular backup procedures

## 🚀 Performance Optimization

### Best Practices
- **Warehouse Sizing**: Use appropriately sized warehouses
- **Task Frequency**: Balance real-time needs with cost
- **Index Strategy**: Consider clustering keys for large tables
- **Query Optimization**: Monitor and optimize query performance

### Scaling Considerations
- **Data Volume**: Monitor table growth and performance
- **Concurrent Users**: Consider dashboard usage patterns
- **Cost Management**: Optimize warehouse usage and task frequency

## 🚀 Key Project Improvements

### Enhanced Architecture
- **Simplified Status Model**: Only BOOKED and CANCELLED statuses for clarity
- **Derived Field Engine**: Automatic business categorization and data quality scoring
- **Raw Stream Preservation**: Complete change history maintained in CDC events table
- **2-Minute Refresh Cycle**: Balanced real-time processing with cost efficiency

### Business Logic Enhancements
- **Revenue Tracking**: Separate active revenue vs lost revenue analysis
- **Booking Categorization**: Size-based (SINGLE/GROUP/LARGE_GROUP) and price-based (BUDGET/STANDARD/PREMIUM) classifications
- **Data Quality Metrics**: Built-in validation and quality scoring
- **Change Tracking**: Comprehensive INSERT/UPDATE/DELETE operation monitoring

### Dashboard Simplification
- **Essential Features Only**: Focused on core analytics without overwhelming complexity
- **Date Range Filtering**: Flexible time-based analysis with September 2025 defaults
- **Clean Interface**: Beginner-friendly design with intuitive navigation
- **Export Capabilities**: CSV download with timestamp for external analysis

## 🔮 Future Enhancements

### Potential Improvements
- **Machine Learning**: Predictive analytics for booking patterns and cancellation prediction
- **Real-time Alerts**: Automated notifications for unusual booking patterns
- **Advanced Visualizations**: More sophisticated chart types and interactive dashboards
- **API Integration**: REST API for external system integration
- **Multi-tenant Support**: Support for multiple movie theaters or locations

### Technical Enhancements
- **Data Lake Integration**: Connect to external data sources for enriched analytics
- **Streaming Analytics**: Real-time stream processing for immediate insights
- **Advanced CDC**: More sophisticated change detection with business rule validation
- **Performance Monitoring**: Enhanced monitoring and alerting for system health

## 📚 Additional Resources

### Documentation
- [Snowflake Streams Documentation](https://docs.snowflake.com/en/user-guide/streams-intro)
- [Snowflake Tasks Documentation](https://docs.snowflake.com/en/user-guide/tasks-intro)
- [Snowflake Dynamic Tables Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables-intro)
- [Streamlit Documentation](https://docs.streamlit.io/)

### Related Projects
- **Healthcare DLT Pipeline**: Delta Live Tables implementation
- **Travel Booking SCD2**: Slowly Changing Dimensions project
- **UPI Transactions CDC**: Real-time transaction processing

## 🤝 Contributing

### Development Guidelines
- Follow SQL best practices and naming conventions
- Add comprehensive comments to all code
- Test changes thoroughly before deployment
- Document any new features or modifications

### Support
For questions or issues with this project, please refer to the troubleshooting section or consult the Snowflake documentation.

---

**🎬 Movie Booking Analytics - Enhanced CDC with Derived Fields & Simplified Dashboard**

**📅 Last Updated:** September 2025  
**🔧 Built with:** Snowflake Streams, Tasks, Dynamic Tables, Streamlit, Python
