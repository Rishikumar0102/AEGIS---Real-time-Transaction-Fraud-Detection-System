# admin_dashboard.py - UPDATED VERSION (No deprecation warnings)
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from sqlalchemy import create_engine
import os

# Page configuration
st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'fraud_detection',
    'user': 'postgres',
    'password': '2005',
    'port': 5432
}

def create_db_engine():
    """Create SQLAlchemy engine for pandas"""
    try:
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)
        return engine
    except Exception as e:
        st.error(f"Database engine error: {e}")
        return None

def get_transactions(limit=1000):
    """Get transactions from database using SQLAlchemy"""
    engine = create_db_engine()
    if engine is None:
        return pd.DataFrame()
    
    query = f"""
    SELECT 
        transaction_id,
        cc_number,
        user_id,
        amount,
        city,
        card_lat,
        card_lon,
        merchant_lat,
        merchant_lon,
        merchant_name,
        timestamp,
        predicted_fraud,
        fraud_score,
        fraud_reason
    FROM transactions 
    ORDER BY timestamp DESC 
    LIMIT {limit}
    """
    
    try:
        df = pd.read_sql_query(query, engine)
        
        # Convert timestamp to datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['date'] = df['timestamp'].dt.date
            df['hour'] = df['timestamp'].dt.hour
        
        return df
    except Exception as e:
        st.error(f"Error fetching transactions: {e}")
        return pd.DataFrame()

def get_dashboard_stats():
    """Get dashboard statistics"""
    engine = create_db_engine()
    if engine is None:
        return {}
    
    try:
        # Total transactions
        total_query = "SELECT COUNT(*) FROM transactions"
        total_transactions = pd.read_sql_query(total_query, engine).iloc[0, 0]
        
        # Fraud transactions
        fraud_query = "SELECT COUNT(*) FROM transactions WHERE predicted_fraud = 1"
        fraud_transactions = pd.read_sql_query(fraud_query, engine).iloc[0, 0]
        
        # Total amount
        total_amount_query = "SELECT COALESCE(SUM(amount), 0) FROM transactions"
        total_amount = pd.read_sql_query(total_amount_query, engine).iloc[0, 0]
        
        # Fraud amount
        fraud_amount_query = "SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE predicted_fraud = 1"
        fraud_amount = pd.read_sql_query(fraud_amount_query, engine).iloc[0, 0]
        
        # Recent fraud rate (last 24 hours)
        recent_query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN predicted_fraud = 1 THEN 1 ELSE 0 END) as fraud
            FROM transactions 
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """
        recent_df = pd.read_sql_query(recent_query, engine)
        recent_total = recent_df.iloc[0, 0] if not recent_df.empty else 0
        recent_fraud = recent_df.iloc[0, 1] if not recent_df.empty else 0
        recent_fraud_rate = (recent_fraud / recent_total * 100) if recent_total > 0 else 0
        
        # SMS Alerts count
        alerts_query = "SELECT COUNT(DISTINCT transaction_id) FROM transactions WHERE fraud_score > 0.85 AND predicted_fraud = 1"
        alerts_count = pd.read_sql_query(alerts_query, engine).iloc[0, 0]
        
        return {
            'total_transactions': total_transactions,
            'fraud_transactions': fraud_transactions,
            'total_amount': total_amount,
            'fraud_amount': fraud_amount,
            'recent_fraud_rate': recent_fraud_rate,
            'alerts_triggered': alerts_count,
            'fraud_percentage': (fraud_transactions / total_transactions * 100) if total_transactions > 0 else 0
        }
    except Exception as e:
        st.error(f"Error fetching stats: {e}")
        return {}

def create_fraud_chart(df):
    """Create fraud chart"""
    if df.empty:
        return go.Figure()
    
    # Prepare data for fraud over time
    fraud_by_hour = df[df['predicted_fraud'] == 1].groupby('hour').size().reset_index(name='count')
    normal_by_hour = df[df['predicted_fraud'] == 0].groupby('hour').size().reset_index(name='count')
    
    fig = go.Figure()
    
    # Add normal transactions
    if not normal_by_hour.empty:
        fig.add_trace(go.Scatter(
            x=normal_by_hour['hour'],
            y=normal_by_hour['count'],
            mode='lines+markers',
            name='Normal Transactions',
            line=dict(color='green', width=2),
            marker=dict(size=8)
        ))
    
    # Add fraud transactions
    if not fraud_by_hour.empty:
        fig.add_trace(go.Scatter(
            x=fraud_by_hour['hour'],
            y=fraud_by_hour['count'],
            mode='lines+markers',
            name='Fraud Transactions',
            line=dict(color='red', width=2),
            marker=dict(size=8, symbol='x')
        ))
    
    fig.update_layout(
        title='Transactions by Hour (Last 24 Hours)',
        xaxis_title='Hour of Day',
        yaxis_title='Number of Transactions',
        height=400,
        template='plotly_dark'
    )
    
    return fig

def create_fraud_reasons_chart(df):
    """Create chart showing fraud reasons"""
    if df.empty or df[df['predicted_fraud'] == 1].empty:
        return go.Figure()
    
    fraud_df = df[df['predicted_fraud'] == 1].copy()
    
    # Extract main reasons
    def extract_main_reason(reason):
        if 'High amount' in str(reason):
            return 'High Amount'
        elif 'Night transaction' in str(reason):
            return 'Night Transaction'
        elif 'ATM at night' in str(reason):
            return 'ATM at Night'
        elif 'Large distance' in str(reason):
            return 'Large Distance'
        elif 'Impossible travel' in str(reason):
            return 'Impossible Travel'
        elif 'Anomaly detected' in str(reason):
            return 'Model Anomaly'
        else:
            return 'Other'
    
    fraud_df.loc[:, 'main_reason'] = fraud_df['fraud_reason'].apply(extract_main_reason)
    reason_counts = fraud_df['main_reason'].value_counts().reset_index()
    reason_counts.columns = ['reason', 'count']
    
    fig = px.pie(
        reason_counts, 
        values='count', 
        names='reason',
        title='Fraud Detection Reasons',
        color_discrete_sequence=px.colors.sequential.RdBu
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(height=400, template='plotly_dark')
    
    return fig

def create_amount_distribution_chart(df):
    """Create amount distribution chart"""
    if df.empty:
        return go.Figure()
    
    fraud_df = df[df['predicted_fraud'] == 1]
    normal_df = df[df['predicted_fraud'] == 0]
    
    fig = go.Figure()
    
    # Add histogram for normal transactions
    if not normal_df.empty:
        fig.add_trace(go.Histogram(
            x=normal_df['amount'],
            name='Normal',
            opacity=0.7,
            marker_color='green',
            nbinsx=50
        ))
    
    # Add histogram for fraud transactions
    if not fraud_df.empty:
        fig.add_trace(go.Histogram(
            x=fraud_df['amount'],
            name='Fraud',
            opacity=0.7,
            marker_color='red',
            nbinsx=50
        ))
    
    fig.update_layout(
        title='Transaction Amount Distribution',
        xaxis_title='Amount (₹)',
        yaxis_title='Count',
        barmode='overlay',
        height=400,
        template='plotly_dark'
    )
    
    return fig

def create_city_heatmap(df):
    """Create fraud heatmap by city"""
    if df.empty:
        return go.Figure()
    
    city_stats = df.groupby('city').agg({
        'transaction_id': 'count',
        'predicted_fraud': 'sum',
        'amount': 'sum'
    }).reset_index()
    
    city_stats['fraud_rate'] = (city_stats['predicted_fraud'] / city_stats['transaction_id'] * 100).round(2)
    
    fig = px.bar(
        city_stats,
        x='city',
        y='fraud_rate',
        color='fraud_rate',
        title='Fraud Rate by City',
        labels={'fraud_rate': 'Fraud Rate (%)'},
        color_continuous_scale='RdYlGn_r'
    )
    
    fig.update_layout(height=400, template='plotly_dark')
    
    return fig

def create_gps_heatmap(df):
    """Create heatmap of fraud transactions by GPS coordinates"""
    if df.empty or df[df['predicted_fraud'] == 1].empty or 'card_lat' not in df.columns:
        return go.Figure()
    
    fraud_df = df[df['predicted_fraud'] == 1].copy()
    
    # Filter out invalid coordinates
    fraud_df = fraud_df.dropna(subset=['card_lat', 'card_lon'])
    
    if fraud_df.empty:
        return go.Figure()
    
    # Updated: Use density_map instead of density_mapbox
    fig = px.density_map(
        fraud_df,
        lat='card_lat',
        lon='card_lon',
        z='amount',
        radius=15,
        center=dict(lat=20.5937, lon=78.9629),  # Center of India
        zoom=3,
        map_style="carto-positron",  # Changed from mapbox_style
        title='Fraud Transactions Heatmap (India)',
        hover_data=['transaction_id', 'city', 'amount', 'fraud_reason']
    )
    
    fig.update_layout(height=500, template='plotly_dark')
    return fig

def create_travel_analysis_chart(df):
    """Create chart showing travel patterns"""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    # Add scatter plot
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['amount'],
        mode='markers',
        marker=dict(
            size=8,
            color=df['predicted_fraud'],
            colorscale=['green', 'red'],
            showscale=True,
            colorbar=dict(title="Fraud Status", tickvals=[0, 1], ticktext=['Normal', 'Fraud'])
        ),
        text=df['city'],
        hovertemplate='<b>%{text}</b><br>Amount: ₹%{y:,.2f}<br>Time: %{x}<br>Fraud: %{marker.color}<extra></extra>'
    ))
    
    fig.update_layout(
        title='Transaction Amounts Over Time (Green=Normal, Red=Fraud)',
        xaxis_title='Time',
        yaxis_title='Amount (₹)',
        height=400,
        template='plotly_dark'
    )
    
    return fig

def create_alerts_log():
    """Create a simple alerts log file system"""
    alerts_file = "fraud_alerts.log"
    
    # Create if not exists
    if not os.path.exists(alerts_file):
        with open(alerts_file, 'w') as f:
            f.write("timestamp,transaction_id,fraud_score,alert_type,status\n")
    
    return alerts_file

def log_alert(transaction_id, fraud_score, alert_types, status="sent"):
    """Log alert to file"""
    alerts_file = create_alerts_log()
    
    with open(alerts_file, 'a') as f:
        timestamp = datetime.now().isoformat()
        alert_str = ",".join(alert_types) if isinstance(alert_types, list) else alert_types
        f.write(f"{timestamp},{transaction_id},{fraud_score},{alert_str},{status}\n")

def display_alerts_history():
    """Display alerts history in Streamlit"""
    alerts_file = create_alerts_log()
    
    if os.path.exists(alerts_file) and os.path.getsize(alerts_file) > 30:
        df = pd.read_csv(alerts_file)
        
        st.subheader("📨 SMS Alert History")
        
        # Summary stats
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total SMS Alerts", len(df))
        with col2:
            sent = len(df[df['status'] == 'sent'])
            st.metric("Successfully Sent", sent)
        with col3:
            failed = len(df[df['status'] == 'failed'])
            st.metric("Failed", failed)
        with col4:
            avg_score = df['fraud_score'].mean() if not df.empty else 0
            st.metric("Avg Fraud Score", f"{avg_score:.1%}")
        
        # Display table
        st.dataframe(
            df.sort_values('timestamp', ascending=False).head(20),
            width='stretch'  # Fixed: Replaced use_container_width
        )
        
        # Chart - Alerts by hour
        if 'timestamp' in df.columns and not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            
            alert_by_hour = df.groupby('hour').size().reset_index(name='count')
            
            if not alert_by_hour.empty:
                fig = px.bar(
                    alert_by_hour,
                    x='hour',
                    y='count',
                    title='SMS Alerts by Hour of Day',
                    labels={'hour': 'Hour', 'count': 'Number of Alerts'},
                    color='count',
                    color_continuous_scale='reds'
                )
                st.plotly_chart(fig, width='stretch')  # Fixed: Replaced use_container_width
    else:
        st.info("No SMS alerts have been sent yet.")

# Custom CSS with auto-refresh indicator
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #FF4B4B;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stat-card {
        background-color: #262730;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .fraud-alert {
        background-color: rgba(255, 75, 75, 0.2);
        border-left: 5px solid #FF4B4B;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 5px;
    }
    .sms-alert {
        background-color: rgba(0, 123, 255, 0.2);
        border-left: 5px solid #007BFF;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 5px;
    }
    .refresh-button {
        background-color: #FF4B4B;
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        border-radius: 5px;
        cursor: pointer;
    }
    .dataframe {
        width: 100%;
    }
    .refresh-indicator {
        position: fixed;
        top: 10px;
        right: 10px;
        background-color: #262730;
        padding: 5px 10px;
        border-radius: 5px;
        font-size: 0.8rem;
        z-index: 9999;
    }
    .alert-badge {
        display: inline-block;
        background-color: #FF4B4B;
        color: white;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.8rem;
        margin-left: 5px;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state for auto-refresh
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()
    st.session_state.refresh_count = 0

# Title and description
st.markdown("<h1 class='main-header'>🚨 Fraud Detection Admin Dashboard</h1>", unsafe_allow_html=True)
st.markdown("""
    Real-time monitoring of credit card transactions with SMS fraud alerts.
    Auto-refreshes every 15 seconds.
""")

# Auto-refresh logic
REFRESH_INTERVAL = 15  # seconds

# Calculate time since last refresh
current_time = datetime.now()
time_since_refresh = (current_time - st.session_state.last_refresh).total_seconds()

# Show refresh indicator
st.markdown(f"""
<div class="refresh-indicator">
    🔄 Next refresh in: <strong>{max(0, REFRESH_INTERVAL - int(time_since_refresh))}s</strong>
</div>
""", unsafe_allow_html=True)

# Refresh button in sidebar
with st.sidebar:
    st.title("Dashboard Controls")
    
    # Manual refresh button
    if st.button("🔄 Manual Refresh", type="primary", width='stretch'):
        st.session_state.last_refresh = datetime.now()
        st.session_state.refresh_count += 1
        st.rerun()
    
    # Auto-refresh toggle
    auto_refresh = st.checkbox("Enable Auto-refresh (15s)", value=True)
    
    if auto_refresh:
        st.info(f"Auto-refresh enabled. Next refresh in {max(0, REFRESH_INTERVAL - int(time_since_refresh))} seconds.")
    
    # Filters
    st.markdown("---")
    st.subheader("Filters")
    
    show_only_fraud = st.checkbox("Show Only Fraud Transactions", value=False)
    
    date_range = st.date_input(
        "Date Range",
        value=[datetime.now().date() - timedelta(days=7), datetime.now().date()],
        max_value=datetime.now().date()
    )
    
    min_amount, max_amount = st.slider(
        "Amount Range (₹)",
        min_value=0,
        max_value=100000,
        value=(0, 50000),
        step=1000
    )
    
    # City filter
    engine = create_db_engine()
    if engine:
        try:
            cities_df = pd.read_sql_query("SELECT DISTINCT city FROM transactions ORDER BY city", engine)
            selected_cities = st.multiselect(
                "Select Cities",
                options=cities_df['city'].tolist(),
                default=[]
            )
        except:
            selected_cities = []
    else:
        selected_cities = []
    
    # Alert Settings
    st.markdown("---")
    st.subheader("🔔 SMS Alert Settings")
    
    # Alert threshold slider
    alert_threshold = st.slider(
        "SMS Alert Threshold",
        min_value=0.7,
        max_value=0.99,
        value=0.85,
        step=0.05,
        help="Minimum fraud score to trigger SMS alerts"
    )
    
    # Manual alert test button
    if st.button("📱 Test SMS Alert System", type="secondary", width='stretch'):
        st.info("Test SMS would be sent to configured admin phone")
        log_alert("TEST_TXN", 0.95, "SMS", "test")
        st.success("Test alert logged! Check alert history below.")
    
    st.markdown("---")
    st.markdown("**Dashboard Status**")
    
    # Show refresh info
    st.metric("Last Refresh", st.session_state.last_refresh.strftime("%H:%M:%S"))
    st.metric("Refresh Count", st.session_state.refresh_count)
    
    # Auto-refresh countdown
    if auto_refresh:
        refresh_in = REFRESH_INTERVAL - int(time_since_refresh)
        if refresh_in <= 0:
            st.info("Auto-refreshing now...")
            st.session_state.last_refresh = datetime.now()
            st.session_state.refresh_count += 1
            time.sleep(0.5)
            st.rerun()
        else:
            st.progress(1 - (refresh_in / REFRESH_INTERVAL), text=f"Next refresh in {refresh_in}s")

# Main dashboard content
col1, col2, col3, col4, col5 = st.columns(5)

# Get statistics
stats = get_dashboard_stats()

with col1:
    st.markdown('<div class="stat-card">', unsafe_allow_html=True)
    st.metric("Total Transactions", f"{stats.get('total_transactions', 0):,}")
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="stat-card">', unsafe_allow_html=True)
    st.metric("Fraud Transactions", f"{stats.get('fraud_transactions', 0):,}", 
              f"{stats.get('fraud_percentage', 0):.1f}%")
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="stat-card">', unsafe_allow_html=True)
    st.metric("SMS Alerts Sent", f"{stats.get('alerts_triggered', 0):,}")
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    st.markdown('<div class="stat-card">', unsafe_allow_html=True)
    st.metric("Total Amount", f"₹{stats.get('total_amount', 0):,.2f}")
    st.markdown('</div>', unsafe_allow_html=True)

with col5:
    st.markdown('<div class="stat-card">', unsafe_allow_html=True)
    st.metric("Fraud Amount", f"₹{stats.get('fraud_amount', 0):,.2f}")
    st.markdown('</div>', unsafe_allow_html=True)

# Get transactions data
transactions_df = get_transactions(limit=1000)

# Apply filters
if not transactions_df.empty:
    # Date filter
    if len(date_range) == 2:
        start_date, end_date = date_range
        mask = (transactions_df['timestamp'].dt.date >= start_date) & (transactions_df['timestamp'].dt.date <= end_date)
        transactions_df = transactions_df[mask].copy()
    
    # Amount filter
    mask = (transactions_df['amount'] >= min_amount) & (transactions_df['amount'] <= max_amount)
    transactions_df = transactions_df[mask].copy()
    
    # Fraud filter
    if show_only_fraud:
        transactions_df = transactions_df[transactions_df['predicted_fraud'] == 1].copy()
    
    # City filter
    if selected_cities:
        transactions_df = transactions_df[transactions_df['city'].isin(selected_cities)].copy()

# Charts section
st.markdown("---")
st.subheader("📊 Analytics Overview")

# Create tabs for different charts
chart_tab1, chart_tab2, chart_tab3, chart_tab4, chart_tab5, chart_tab6 = st.tabs([
    "📈 Transaction Trends", 
    "🎯 Fraud Reasons", 
    "💰 Amount Analysis", 
    "🌍 Geographic Analysis",
    "📍 GPS Heatmap",
    "📊 Travel Patterns"
])

with chart_tab1:
    fig1 = create_fraud_chart(transactions_df)
    st.plotly_chart(fig1, width='stretch')  # Fixed

with chart_tab2:
    fig2 = create_fraud_reasons_chart(transactions_df)
    st.plotly_chart(fig2, width='stretch')  # Fixed

with chart_tab3:
    fig3 = create_amount_distribution_chart(transactions_df)
    st.plotly_chart(fig3, width='stretch')  # Fixed

with chart_tab4:
    fig4 = create_city_heatmap(transactions_df)
    st.plotly_chart(fig4, width='stretch')  # Fixed

with chart_tab5:
    fig5 = create_gps_heatmap(transactions_df)
    st.plotly_chart(fig5, width='stretch')  # Fixed

with chart_tab6:
    fig6 = create_travel_analysis_chart(transactions_df)
    st.plotly_chart(fig6, width='stretch')  # Fixed

# SMS Alerts History Section
st.markdown("---")
display_alerts_history()

# Recent transactions table
st.markdown("---")
st.subheader("🔍 Recent Transactions")

if not transactions_df.empty:
    # Format the dataframe for display
    display_df = transactions_df.copy()
    display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    display_df['amount'] = display_df['amount'].apply(lambda x: f"₹{x:,.2f}")
    display_df['fraud_score'] = display_df['fraud_score'].apply(lambda x: f"{x:.2%}")
    
    # Add SMS alert indicator
    def add_sms_indicator(row):
        fraud_score = float(row['fraud_score'].strip('%')) / 100 if '%' in str(row['fraud_score']) else 0
        if fraud_score > alert_threshold and row['predicted_fraud'] == 1:
            return "📱 SMS Sent"
        elif row['predicted_fraud'] == 1:
            return "⚠️ Fraud"
        else:
            return "✅ Normal"
    
    display_df['status'] = display_df.apply(add_sms_indicator, axis=1)
    
    # Color code rows
    def highlight_rows(row):
        if row['status'] == '📱 SMS Sent':
            return ['background-color: rgba(0, 123, 255, 0.2)'] * len(row)
        elif row['status'] == '⚠️ Fraud':
            return ['background-color: rgba(255, 75, 75, 0.2)'] * len(row)
        else:
            return [''] * len(row)
    
    # Select columns to display
    display_columns = ['timestamp', 'transaction_id', 'user_id', 'city', 
                       'amount', 'status', 'fraud_score', 'fraud_reason']
    
    # Filter to available columns
    display_columns = [col for col in display_columns if col in display_df.columns]
    
    styled_df = display_df[display_columns].style.apply(highlight_rows, axis=1)
    
    st.dataframe(
        styled_df,
        width='stretch',  # Fixed: Replaced use_container_width
        height=400
    )
    
    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Transactions as CSV",
        data=csv,
        file_name=f"fraud_transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        width='stretch'  # Fixed
    )
else:
    st.info("No transactions found with the current filters.")

# Real-time SMS Alerts section
st.markdown("---")
st.subheader("📱 Real-time SMS Alert Monitor")

# Simulate real-time SMS alerts
if not transactions_df.empty and (transactions_df['predicted_fraud'] == 1).any():
    # Get recent high-confidence frauds that would trigger SMS
    recent_sms_alerts = transactions_df[
        (transactions_df['predicted_fraud'] == 1) & 
        (transactions_df['fraud_score'] > alert_threshold)
    ].head(3)
    
    if not recent_sms_alerts.empty:
        st.markdown(f"**Last {len(recent_sms_alerts)} SMS Alerts (Fraud > {alert_threshold:.0%}):**")
        
        for _, alert in recent_sms_alerts.iterrows():
            st.markdown(f"""
            <div class="sms-alert">
                <strong>📱 SMS Alert Sent!</strong><br>
                <strong>Transaction ID:</strong> {alert['transaction_id']}<br>
                <strong>User:</strong> {alert['user_id']} | <strong>Card:</strong> {alert['cc_number'][-4:]}<br>
                <strong>Amount:</strong> ₹{alert['amount']:,.2f} | <strong>Location:</strong> {alert['city']}<br>
                <strong>Time:</strong> {alert['timestamp'].strftime('%H:%M:%S')}<br>
                <strong>Fraud Score:</strong> {(alert['fraud_score'] * 100):.1f}%<br>
                <strong>Reason:</strong> {alert['fraud_reason']}
            </div>
            """, unsafe_allow_html=True)
    else:
        st.success("✅ No recent high-confidence fraud requiring SMS alerts")
else:
    st.success("✅ No fraud detected recently")

# JavaScript for auto-refresh (15 seconds)
st.markdown(f"""
<script>
// Auto-refresh every 15 seconds
const refreshInterval = {REFRESH_INTERVAL * 1000}; // Convert to milliseconds

function updateCountdown() {{
    const countdownElement = document.querySelector('.refresh-indicator strong');
    if (countdownElement) {{
        let seconds = parseInt(countdownElement.textContent);
        if (seconds > 0) {{
            seconds--;
            countdownElement.textContent = seconds + 's';
        }}
    }}
}}

// Update countdown every second
setInterval(updateCountdown, 1000);

// Full page refresh after interval
setTimeout(function(){{
    window.location.reload();
}}, refreshInterval);
</script>
""", unsafe_allow_html=True)

# Update last refresh time
st.session_state.last_refresh = datetime.now()