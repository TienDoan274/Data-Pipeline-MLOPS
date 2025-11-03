# dashboard/app_simple_pubsub.py
"""
Simple E-commerce Dashboard with Redis Pub/Sub Auto-Refresh
- No manual refresh needed
- Auto-updates when DAG completes
- Clean and simple
"""
import streamlit as st
import pandas as pd
import boto3
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import redis
import json
import time

# Page config
st.set_page_config(
    page_title="📊 E-commerce Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
    }
    .update-badge {
        position: fixed;
        top: 70px;
        right: 20px;
        background: #4CAF50;
        color: white;
        padding: 10px 20px;
        border-radius: 20px;
        z-index: 1000;
        animation: fadeIn 0.5s;
    }
    @keyframes fadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
    }
</style>
""", unsafe_allow_html=True)

# Redis client
@st.cache_resource
def get_redis_client():
    return redis.Redis(host='redis', port=6379, decode_responses=True)

# MinIO client
@st.cache_resource
def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1'
    )

# Check for new data from Redis
def check_for_updates():
    """Check if new data available via Redis marker"""
    redis_client = get_redis_client()
    try:
        last_update = redis_client.get('dashboard:last_update')
        return last_update
    except:
        return None

# Load dashboard metrics
@st.cache_data(ttl=60)
def load_dashboard_metrics():
    """Load pre-computed metrics from MinIO"""
    s3 = get_s3_client()
    
    metrics = {}
    metric_files = [
        'overall_metrics',
        'top_products',
        'category_stats',
        'regional_stats',
        'hourly_stats'
    ]
    
    for metric_name in metric_files:
        try:
            obj = s3.get_object(
                Bucket='gold',
                Key=f'dashboard/metrics/{metric_name}.parquet'
            )
            metrics[metric_name] = pd.read_parquet(BytesIO(obj['Body'].read()))
        except Exception as e:
            metrics[metric_name] = pd.DataFrame()
    
    return metrics

# Initialize session state
if 'last_check' not in st.session_state:
    st.session_state.last_check = None
if 'refresh_count' not in st.session_state:
    st.session_state.refresh_count = 0

# Check for updates
current_update = check_for_updates()
if current_update and current_update != st.session_state.last_check:
    st.session_state.last_check = current_update
    st.cache_data.clear()
    st.session_state.refresh_count += 1
    if st.session_state.last_check:  # Not first load
        st.toast("✅ New data received!", icon="✅")

# ============ SIDEBAR ============
with st.sidebar:
    st.title("⚙️ Dashboard Settings")
    
    # DAG Info
    st.subheader("⚡ DAG Status")
    st.info("**Schedule:** Every 30 seconds")
    st.caption("Auto-refreshes via Redis pub/sub")
    
    st.divider()
    
    # Statistics
    st.subheader("📊 Statistics")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Updates", st.session_state.refresh_count)
    with col2:
        if current_update:
            try:
                update_dt = datetime.fromisoformat(current_update)
                st.caption(f"Last: {update_dt.strftime('%H:%M:%S')}")
            except:
                pass
    
    st.divider()
    
    # Auto-refresh settings
    st.subheader("🔄 Auto-Refresh")
    
    auto_refresh = st.checkbox("Enable auto-refresh", value=True, help="Check for updates automatically")
    
    if auto_refresh:
        refresh_interval = st.slider(
            "Check interval (seconds)",
            min_value=5,
            max_value=60,
            value=10,
            step=5,
            help="How often to check for new data"
        )
    
    st.divider()
    
    # Manual actions
    st.subheader("⚡ Actions")
    
    if st.button("🔄 Force Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.caption(f"Last checked: {datetime.now().strftime('%H:%M:%S')}")

# ============ MAIN CONTENT ============
st.title("📊 E-commerce Real-time Dashboard")
st.caption("🔄 Auto-updates via Redis pub/sub • Data refreshes every 30 seconds")

# Status indicator
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    if current_update:
        try:
            update_dt = datetime.fromisoformat(current_update)
            st.success(f"✅ Live • Last update: {update_dt.strftime('%H:%M:%S')}")
        except:
            st.info("🔄 Waiting for data...")
    else:
        st.info("🔄 Waiting for first update...")

with col2:
    st.caption(f"📊 Updates: {st.session_state.refresh_count}")

with col3:
    if auto_refresh:
        st.caption(f"⏱️ Check: {refresh_interval}s")

st.divider()

# Load data
with st.spinner("Loading dashboard metrics..."):
    metrics = load_dashboard_metrics()

# Check if data available
if metrics['overall_metrics'].empty:
    st.warning("⚠️ No data available yet.")
    st.info("💡 Waiting for DAG to complete first run (every 30 seconds)...")
    
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()
    st.stop()

overall = metrics['overall_metrics'].iloc[0]

# ============ TOP METRICS ============
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="📦 Total Orders Today",
        value=f"{int(overall['total_orders']):,}"
    )

with col2:
    st.metric(
        label="💰 Total Revenue",
        value=f"${overall['total_revenue']:,.2f}"
    )

with col3:
    st.metric(
        label="🛒 Avg Order Value",
        value=f"${overall['avg_order_value']:,.2f}"
    )

with col4:
    st.metric(
        label="👥 Unique Customers",
        value=f"{int(overall['unique_customers']):,}"
    )

st.divider()

# ============ TABS ============
tab1, tab2, tab3 = st.tabs([
    "📈 Today's Performance",
    "🏆 Top Products",
    "📊 Categories & Regions"
])

# TAB 1: Today's Performance
with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⏰ Hourly Orders")
        if not metrics['hourly_stats'].empty:
            fig = px.line(
                metrics['hourly_stats'],
                x='hour',
                y='order_count',
                markers=True,
                title="Orders by Hour"
            )
            fig.update_layout(
                xaxis_title="Hour of Day",
                yaxis_title="Number of Orders",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💵 Hourly Revenue")
        if not metrics['hourly_stats'].empty:
            fig = px.bar(
                metrics['hourly_stats'],
                x='hour',
                y='revenue',
                title="Revenue by Hour"
            )
            fig.update_layout(
                xaxis_title="Hour of Day",
                yaxis_title="Revenue ($)",
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

# TAB 2: Top Products
with tab2:
    st.subheader("🏆 Top 10 Products Today")
    
    if not metrics['top_products'].empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = px.bar(
                metrics['top_products'],
                x='revenue',
                y='product_name',
                orientation='h',
                title="Top Products by Revenue",
                color='revenue',
                color_continuous_scale='viridis'
            )
            fig.update_layout(
                xaxis_title="Revenue ($)",
                yaxis_title="Product",
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.dataframe(
                metrics['top_products'][['product_name', 'order_count', 'revenue']]
                .rename(columns={
                    'product_name': 'Product',
                    'order_count': 'Orders',
                    'revenue': 'Revenue'
                }),
                use_container_width=True,
                hide_index=True
            )

# TAB 3: Categories & Regions
with tab3:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📦 Category Performance")
        if not metrics['category_stats'].empty:
            fig = px.pie(
                metrics['category_stats'],
                values='revenue',
                names='category',
                title="Revenue by Category",
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(
                metrics['category_stats']
                .sort_values('revenue', ascending=False)
                .rename(columns={
                    'category': 'Category',
                    'order_count': 'Orders',
                    'revenue': 'Revenue',
                    'quantity_sold': 'Items Sold'
                }),
                use_container_width=True,
                hide_index=True
            )
    
    with col2:
        st.subheader("🌍 Regional Performance")
        if not metrics['regional_stats'].empty:
            fig = px.bar(
                metrics['regional_stats'].sort_values('revenue', ascending=False),
                x='region',
                y='revenue',
                title="Revenue by Region",
                color='revenue',
                color_continuous_scale='blues'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(
                metrics['regional_stats']
                .sort_values('revenue', ascending=False)
                .rename(columns={
                    'region': 'Region',
                    'order_count': 'Orders',
                    'revenue': 'Revenue'
                }),
                use_container_width=True,
                hide_index=True
            )

# ============ AUTO-REFRESH ============
if auto_refresh:
    # Progress indicator
    progress_placeholder = st.empty()
    
    with progress_placeholder.container():
        st.info(f"🔄 Next check in {refresh_interval}s")
        progress_bar = st.progress(0)
        
        for remaining in range(refresh_interval, 0, -1):
            progress = (refresh_interval - remaining) / refresh_interval
            progress_bar.progress(progress)
            time.sleep(1)
        
        progress_bar.empty()
    
    progress_placeholder.empty()
    st.rerun()