# =====================================================
# Movies CDC Dynamic Tables - Simple Streamlit Dashboard
# =====================================================
# A beginner-friendly dashboard for movie booking analytics
# using Snowflake CDC and Dynamic Tables

# Import required packages
from snowflake.snowpark.context import get_active_session
import streamlit as st
from datetime import date, datetime
from snowflake.snowpark.functions import col
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# =====================================================
# PAGE CONFIGURATION
# =====================================================
st.set_page_config(
    page_title="Movie Booking Analytics Dashboard",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =====================================================
# SNOWFLAKE CONNECTION
# =====================================================
# Get the active Snowpark session for Snowflake connectivity
try:
    session = get_active_session()
    st.success("✅ Connected to Snowflake successfully!")
except Exception as e:
    st.error(f"❌ Failed to connect to Snowflake: {str(e)}")
    st.stop()

# =====================================================
# DASHBOARD HEADER
# =====================================================
st.title("🎬 Movie Booking Analytics Dashboard")
st.markdown("**Real-time CDC Analytics powered by Snowflake Dynamic Tables**")
st.markdown("---")

# =====================================================
# SIMPLE SIDEBAR FILTERS
# =====================================================
st.sidebar.header("🔍 Filters")

# Date range filter
date_range = st.sidebar.date_input(
    "📅 Date Range:",
    value=(date(2025, 9, 1), date(2025, 9, 30)),
    help="Select booking date range"
)

# Simple status filter
status_options = ["All", "BOOKED", "CANCELLED"]
selected_status = st.sidebar.selectbox("🎫 Booking Status:", status_options)

# Simple movie filter
try:
    movies_df = session.table("movie_booking_insights").select("movie_id").distinct().to_pandas()
    movie_options = ["All"] + movies_df["movie_id"].tolist()
    selected_movie = st.sidebar.selectbox("🎬 Movie:", movie_options)
except:
    selected_movie = "All"

# Refresh button
if st.sidebar.button("🔄 Refresh"):
    st.rerun()

# =====================================================
# SIMPLE DATA LOADING
# =====================================================
try:
    # Load data from filtered table
    data_df = session.table("movie_bookings_filtered")
    
    # Apply date range filter
    if len(date_range) == 2 and date_range[0] and date_range[1]:
        start_date, end_date = date_range
        data_df = data_df.filter(
            (col("booking_date") >= start_date) & (col("booking_date") <= end_date)
        )
    
    # Apply status filter
    if selected_status != "All":
        data_df = data_df.filter(col("status") == selected_status)
    
    # Apply movie filter
    if selected_movie != "All":
        data_df = data_df.filter(col("movie_id") == selected_movie)
    
    # Convert to pandas
    df = data_df.to_pandas()
    df.columns = [col.lower() for col in df.columns]
    
    # Load insights
    insights_df = session.table("movie_booking_insights").to_pandas()
    insights_df.columns = [col.lower() for col in insights_df.columns]
    
except Exception as e:
    st.error(f"❌ Error: {str(e)}")
    st.stop()

# =====================================================
# KEY METRICS
# =====================================================
if not df.empty:
    st.subheader("📊 Key Metrics")
    
    # Calculate key metrics
    total_bookings = len(df)
    total_revenue = df["total_amount"].sum()
    active_revenue = df["active_revenue"].sum()
    lost_revenue = df["lost_revenue"].sum()
    
    # Simple 4-column layout
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Bookings", total_bookings)
    
    with col2:
        st.metric("Total Revenue", f"${total_revenue:,.2f}")
    
    with col3:
        st.metric("Active Revenue", f"${active_revenue:,.2f}")
    
    with col4:
        st.metric("Lost Revenue", f"${lost_revenue:,.2f}")
    
    st.markdown("---")
    
    # =====================================================
    # MAIN CHARTS
    # =====================================================
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("📈 Revenue by Status")
        
        # Simple revenue chart
        revenue_data = df.groupby("status")["total_amount"].sum().reset_index()
        fig_revenue = px.bar(
            revenue_data, 
            x="status", 
            y="total_amount",
            title="Revenue by Booking Status",
            color="status",
            color_discrete_map={"BOOKED": "#32CD32", "CANCELLED": "#FF6347"}
        )
        st.plotly_chart(fig_revenue, use_container_width=True)
    
    with col_right:
        st.subheader("🎫 Booking Distribution")
        
        # Simple status distribution
        status_counts = df["status"].value_counts()
        fig_pie = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Booking Status Distribution",
            color_discrete_map={"BOOKED": "#32CD32", "CANCELLED": "#FF6347"}
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    # =====================================================
    # MOVIE PERFORMANCE
    # =====================================================
    st.subheader("🎬 Movie Performance")
    
    # Simple movie performance table
    movie_performance = df.groupby("movie_id").agg({
        "booking_id": "count",
        "total_amount": "sum",
        "ticket_count": "sum"
    }).reset_index()
    movie_performance.columns = ["Movie ID", "Bookings", "Revenue", "Tickets"]
    
    # Display table
    st.dataframe(
        movie_performance.sort_values("Revenue", ascending=False),
        use_container_width=True
    )
    
    # =====================================================
    # CDC INSIGHTS
    # =====================================================
    if not insights_df.empty:
        st.subheader("🔄 Real-time Insights")
        
        # Simple insights display
        insights_display = insights_df[[
            "movie_id", "total_bookings", "total_active_revenue", 
            "active_bookings", "cancelled_bookings", "cancellation_rate_percent"
        ]].copy()
        insights_display.columns = [
            "Movie ID", "Total Bookings", "Active Revenue",
            "Active Bookings", "Cancelled Bookings", "Cancellation Rate %"
        ]
        
        st.dataframe(
            insights_display.sort_values("Active Revenue", ascending=False),
            use_container_width=True
        )
    
    # =====================================================
    # DATA VIEW
    # =====================================================
    with st.expander("📋 View Raw Data"):
        st.dataframe(df, use_container_width=True)
        
        # Download option
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"movie_bookings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

else:
    st.warning("⚠️ No data available for the selected filters.")

# =====================================================
# FOOTER
# =====================================================
st.markdown("---")
st.markdown("**🔧 Powered by Snowflake Dynamic Tables & Streamlit**")
st.markdown(f"**🕒 Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.markdown("🔄 *Data refreshes automatically every 2 minutes*")