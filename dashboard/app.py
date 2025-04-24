#!/usr/bin/env python3
"""
Streamlit dashboard for audiobook engagement analytics.
Visualizes user and book engagement metrics using Athena queries.
"""

import os
import time
from typing import Dict, List, Optional, Tuple
import json

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import boto3
from botocore.exceptions import ClientError

# AWS Configuration
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE', 'audiobook_analytics')
ATHENA_TABLE = os.environ.get('ATHENA_TABLE', 'playback_events')
ATHENA_OUTPUT = os.environ.get('ATHENA_OUTPUT', 's3://audio-engagement-data/athena-results/')

# Initialize AWS clients
athena = boto3.client('athena', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)

def run_query(query: str) -> Optional[pd.DataFrame]:
    """
    Execute Athena query and return results as DataFrame.
    
    Args:
        query: SQL query to execute
        
    Returns:
        DataFrame containing query results or None if query fails
    """
    try:
        # Start query execution
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        query_id = response['QueryExecutionId']
        
        # Wait for query to complete
        while True:
            response = athena.get_query_execution(QueryExecutionId=query_id)
            state = response['QueryExecution']['Status']['State']
            
            if state == 'SUCCEEDED':
                # Get results
                results = athena.get_query_results(QueryExecutionId=query_id)
                
                # Convert to DataFrame
                columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                rows = []
                for row in results['ResultSet']['Rows'][1:]:  # Skip header
                    rows.append([field.get('VarCharValue', '') for field in row['Data']])
                
                return pd.DataFrame(rows, columns=columns)
                
            elif state in ['FAILED', 'CANCELLED']:
                error = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                st.error(f"Query failed: {error}")
                return None
                
            time.sleep(1)
            
    except Exception as e:
        st.error(f"Failed to execute query: {e}")
        return None

def get_user_metrics(user_id: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Get engagement metrics for a specific user."""
    
    # Event counts
    events_query = f"""
    SELECT 
        event_type,
        COUNT(*) as count
    FROM {ATHENA_TABLE}
    WHERE user_id = '{user_id}'
    GROUP BY event_type
    ORDER BY count DESC
    """
    events_df = run_query(events_query)
    
    # Top books
    books_query = f"""
    WITH book_events AS (
        SELECT 
            book_id,
            COUNT(*) as total_events,
            SUM(CASE WHEN event_type = 'complete' THEN 1 ELSE 0 END) as completions,
            SUM(CASE WHEN event_type = 'skip' THEN 1 ELSE 0 END) as skips
        FROM {ATHENA_TABLE}
        WHERE user_id = '{user_id}'
        GROUP BY book_id
    )
    SELECT *,
        (total_events - skips) * 1.0 / NULLIF(total_events, 0) as engagement_score
    FROM book_events
    ORDER BY engagement_score DESC
    LIMIT 5
    """
    books_df = run_query(books_query)
    
    # Session duration trend
    sessions_query = f"""
    WITH session_events AS (
        SELECT 
            DATE_TRUNC('day', timestamp) as date,
            book_id,
            timestamp,
            LAG(timestamp) OVER (PARTITION BY book_id ORDER BY timestamp) as prev_time
        FROM {ATHENA_TABLE}
        WHERE 
            user_id = '{user_id}'
            AND event_type IN ('start', 'pause', 'resume', 'complete')
    )
    SELECT 
        date,
        AVG(
            CASE 
                WHEN DATE_DIFF('hour', prev_time, timestamp) < 4 
                THEN DATE_DIFF('minute', prev_time, timestamp)
                ELSE NULL 
            END
        ) as avg_session_minutes
    FROM session_events
    WHERE prev_time IS NOT NULL
    GROUP BY date
    ORDER BY date
    """
    sessions_df = run_query(sessions_query)
    
    return events_df, books_df, sessions_df

def get_book_metrics(book_id: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Get engagement metrics for a specific book."""
    
    # Event counts
    events_query = f"""
    SELECT 
        event_type,
        COUNT(*) as count
    FROM {ATHENA_TABLE}
    WHERE book_id = '{book_id}'
    GROUP BY event_type
    ORDER BY count DESC
    """
    events_df = run_query(events_query)
    
    # Top users
    users_query = f"""
    WITH user_events AS (
        SELECT 
            user_id,
            COUNT(*) as total_events,
            SUM(CASE WHEN event_type = 'complete' THEN 1 ELSE 0 END) as completions,
            SUM(CASE WHEN event_type = 'skip' THEN 1 ELSE 0 END) as skips
        FROM {ATHENA_TABLE}
        WHERE book_id = '{book_id}'
        GROUP BY user_id
    )
    SELECT *,
        (total_events - skips) * 1.0 / NULLIF(total_events, 0) as engagement_score
    FROM user_events
    ORDER BY engagement_score DESC
    LIMIT 5
    """
    users_df = run_query(users_query)
    
    # Daily engagement trend
    trend_query = f"""
    SELECT 
        DATE_TRUNC('day', timestamp) as date,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(*) as total_events,
        SUM(CASE WHEN event_type = 'complete' THEN 1 ELSE 0 END) as completions
    FROM {ATHENA_TABLE}
    WHERE book_id = '{book_id}'
    GROUP BY DATE_TRUNC('day', timestamp)
    ORDER BY date
    """
    trend_df = run_query(trend_query)
    
    return events_df, users_df, trend_df

def plot_event_distribution(df: pd.DataFrame, title: str) -> None:
    """Plot event type distribution as a pie chart."""
    if df is not None and not df.empty:
        fig = px.pie(
            df,
            values='count',
            names='event_type',
            title=title,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig)
    else:
        st.warning("No event data available")

def plot_top_items(df: pd.DataFrame, id_col: str, title: str) -> None:
    """Plot top items by engagement score as a bar chart."""
    if df is not None and not df.empty:
        fig = px.bar(
            df,
            x=id_col,
            y='engagement_score',
            title=title,
            color='completions',
            hover_data=['total_events', 'skips'],
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig)
    else:
        st.warning("No engagement data available")

def plot_time_series(df: pd.DataFrame, y_col: str, title: str) -> None:
    """Plot time series data as a line chart."""
    if df is not None and not df.empty:
        fig = px.line(
            df,
            x='date',
            y=y_col,
            title=title,
            markers=True
        )
        fig.update_traces(line_color='#2E86C1')
        st.plotly_chart(fig)
    else:
        st.warning("No time series data available")

def main():
    """Main Streamlit app."""
    st.set_page_config(
        page_title="Audiobook Engagement Analytics",
        page_icon="📚",
        layout="wide"
    )
    
    st.title("📚 Audiobook Engagement Analytics")
    st.markdown("""
    Analyze user engagement with audiobooks using metrics like plays, skips, and completions.
    Enter a user ID to see their listening patterns or a book ID to see its performance.
    """)
    
    # Input selection
    col1, col2 = st.columns(2)
    with col1:
        search_type = st.radio("Search by:", ["User ID", "Book ID"])
    
    with col2:
        search_id = st.text_input(
            "Enter ID",
            placeholder="Enter user_id or book_id",
            key="search_id"
        )
    
    if search_id:
        with st.spinner("Fetching data..."):
            if search_type == "User ID":
                events_df, items_df, time_df = get_user_metrics(search_id)
                st.subheader(f"User Analytics: {search_id}")
                
                # Display metrics in columns
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown("### Event Distribution")
                    plot_event_distribution(events_df, "Event Types")
                    
                with col2:
                    st.markdown("### Top Books")
                    plot_top_items(items_df, 'book_id', "Most Engaged Books")
                    
                with col3:
                    st.markdown("### Session Duration Trend")
                    plot_time_series(time_df, 'avg_session_minutes', "Average Session Duration (minutes)")
                    
            else:  # Book ID
                events_df, items_df, time_df = get_book_metrics(search_id)
                st.subheader(f"Book Analytics: {search_id}")
                
                # Display metrics in columns
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown("### Event Distribution")
                    plot_event_distribution(events_df, "Event Types")
                    
                with col2:
                    st.markdown("### Top Users")
                    plot_top_items(items_df, 'user_id', "Most Engaged Users")
                    
                with col3:
                    st.markdown("### Daily Engagement")
                    plot_time_series(time_df, 'unique_users', "Daily Active Users")
                    
            # Show raw data tables
            if st.checkbox("Show raw data"):
                st.markdown("### Raw Data")
                tab1, tab2, tab3 = st.tabs(["Events", "Rankings", "Trends"])
                
                with tab1:
                    st.dataframe(events_df)
                with tab2:
                    st.dataframe(items_df)
                with tab3:
                    st.dataframe(time_df)

if __name__ == "__main__":
    main() 