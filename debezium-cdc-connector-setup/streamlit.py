import streamlit as st
from kafka import KafkaConsumer
import pandas as pd
import json
import base64
import time
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ============================================================
#     Decimal Fix — Decode DECIMAL from Debezium Base64
# ============================================================
def decode_decimal_from_base64(b64_value, scale=2):
    """Decode SQL Server DECIMAL coming from Debezium (Base64 binary)."""
    if not b64_value or b64_value in ["", "AA=="]:
        return None

    try:
        if isinstance(b64_value, str) and b64_value.replace('.', '', 1).replace('-', '', 1).isdigit():
            return float(b64_value)
    except:
        pass

    try:
        raw_bytes = base64.b64decode(b64_value)
        int_value = int.from_bytes(raw_bytes, byteorder="big", signed=True)
        return int_value / (10 ** scale)
    except Exception as e:
        return None


decimal_cols_scale = {
    "temperature": 2,
    "humidity": 2,
    "wind_speed": 2,
    "wind_direction": 2,
    "pressure": 2,
    "precipitation": 2,
    "visibility": 2,
    "cloud_coverage": 2
}


# ============================================================
#     Consume Topic with ALL Historical Data
# ============================================================
def consume_topic(topic_name, bootstrap_servers, fields_decimal={}, include_snapshot=True):
    """Consume ALL messages from Kafka topic (historical + new)"""
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
            auto_offset_reset="earliest",  # Start from beginning
            enable_auto_commit=True,
            consumer_timeout_ms=5000,
            max_poll_records=500
        )

        records = []
        for msg in consumer:
            # Skip if message value is None
            if msg.value is None:
                continue
                
            rec = msg.value
            op_type = rec.get("op")

            if include_snapshot:
                if op_type not in ["c", "u", "r"]:
                    continue
            else:
                if op_type not in ["c", "u"]:
                    continue

            after = rec.get("after", {})
            if after:
                row = {}
                for k, v in after.items():
                    if k in fields_decimal:
                        scale = fields_decimal[k]
                        row[k] = decode_decimal_from_base64(v, scale)
                    else:
                        row[k] = v
                records.append(row)

        consumer.close()
        return pd.DataFrame(records)
    
    except Exception as e:
        st.error(f"Error consuming topic {topic_name}: {str(e)}")
        return pd.DataFrame()


# ============================================================
#     Initialize Session State
# ============================================================
if 'all_data' not in st.session_state:
    st.session_state.all_data = pd.DataFrame()
if 'consumer_fact' not in st.session_state:
    st.session_state.consumer_fact = None
if 'is_streaming' not in st.session_state:
    st.session_state.is_streaming = False
if 'dim_location' not in st.session_state:
    st.session_state.dim_location = pd.DataFrame()
if 'dim_time' not in st.session_state:
    st.session_state.dim_time = pd.DataFrame()
if 'dim_event' not in st.session_state:
    st.session_state.dim_event = pd.DataFrame()
if 'dim_model' not in st.session_state:
    st.session_state.dim_model = pd.DataFrame()
if 'last_offset' not in st.session_state:
    st.session_state.last_offset = 0


# ============================================================
#     Streamlit UI
# ============================================================
st.set_page_config(page_title="Weather Analytics Dashboard", layout="wide")
st.title("📊 Weather Data Analytics Dashboard - Historical & Real-Time")

bootstrap_servers = ["localhost:9092"]

# ============================================================
#     Sidebar Controls
# ============================================================
st.sidebar.header("⚙️ Data Loading & Controls")

# Step 1: Load ALL Historical Data
if st.sidebar.button("📥 Load ALL Historical Data", type="primary"):
    with st.spinner("Loading all historical data from Kafka..."):
        # Load dimensions
        st.session_state.dim_location = consume_topic("KASSABY.dbo.DimLocation", bootstrap_servers, include_snapshot=True)
        st.session_state.dim_time = consume_topic("KASSABY.dbo.DimTime", bootstrap_servers, include_snapshot=True)
        st.session_state.dim_event = consume_topic("KASSABY.dbo.DimWeatherEvent", bootstrap_servers, include_snapshot=True)
        st.session_state.dim_model = consume_topic("KASSABY.dbo.DimForecastModel", bootstrap_servers, include_snapshot=True)
        
        # Load ALL fact data
        df_fact = consume_topic("KASSABY.dbo.FactWeatherObservation", bootstrap_servers, 
                               fields_decimal=decimal_cols_scale, include_snapshot=True)
        
        if not df_fact.empty:
            # Merge with dimensions
            df = df_fact.copy()
            
            if not st.session_state.dim_location.empty and "location_id" in df.columns:
                df = df.merge(st.session_state.dim_location, on="location_id", how="left", suffixes=('', '_loc'))
            
            if not st.session_state.dim_time.empty and "time_id" in df.columns:
                df = df.merge(st.session_state.dim_time, on="time_id", how="left", suffixes=('', '_time'))
            
            if not st.session_state.dim_event.empty and "event_id" in df.columns:
                df = df.merge(st.session_state.dim_event, on="event_id", how="left", suffixes=('', '_event'))
            
            if not st.session_state.dim_model.empty and "model_id" in df.columns:
                df = df.merge(st.session_state.dim_model, on="model_id", how="left", suffixes=('', '_model'))
            
            df['_loaded_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state.all_data = df
            st.sidebar.success(f"✅ Loaded {len(df):,} historical records!")
        else:
            st.sidebar.warning("No historical data found")

# Display data stats
if not st.session_state.all_data.empty:
    st.sidebar.info(f"📊 Total Records: {len(st.session_state.all_data):,}")
    st.sidebar.info(f"📍 Locations: {len(st.session_state.dim_location)}")
    st.sidebar.info(f"☁️ Weather Events: {len(st.session_state.dim_event)}")

st.sidebar.divider()

# Step 2: Real-time streaming
st.sidebar.subheader("🔴 Real-Time Streaming")

col_start, col_stop = st.sidebar.columns(2)

if col_start.button("▶️ Start"):
    st.session_state.is_streaming = True
    st.rerun()

if col_stop.button("⏸️ Stop"):
    st.session_state.is_streaming = False
    if st.session_state.consumer_fact:
        try:
            st.session_state.consumer_fact.close()
        except:
            pass
        st.session_state.consumer_fact = None

if st.session_state.is_streaming:
    st.sidebar.success("🟢 Streaming Active")
else:
    st.sidebar.warning("🔴 Streaming Stopped")

if st.sidebar.button("🗑️ Clear All Data"):
    st.session_state.all_data = pd.DataFrame()
    st.session_state.is_streaming = False
    st.rerun()


# ============================================================
#     Real-Time Streaming Handler
# ============================================================
if st.session_state.is_streaming:
    if st.session_state.consumer_fact is None:
        try:
            st.session_state.consumer_fact = KafkaConsumer(
                "KASSABY.dbo.FactWeatherObservation",
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
                auto_offset_reset="latest",  # Only new messages
                enable_auto_commit=True,
                consumer_timeout_ms=1000
            )
        except Exception as e:
            st.error(f"Failed to connect: {str(e)}")
            st.session_state.is_streaming = False

    if st.session_state.consumer_fact:
        try:
            messages = st.session_state.consumer_fact.poll(timeout_ms=1000, max_records=10)
            
            new_records = []
            for topic_partition, msgs in messages.items():
                for msg in msgs:
                    # Skip if message value is None
                    if msg.value is None:
                        continue
                        
                    rec = msg.value
                    if rec.get("op") in ["c", "u"]:
                        after = rec.get("after", {})
                        row = {}
                        for k, v in after.items():
                            if k in decimal_cols_scale:
                                row[k] = decode_decimal_from_base64(v, decimal_cols_scale[k])
                            else:
                                row[k] = v
                        row['_loaded_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        new_records.append(row)
            
            if new_records:
                df_new = pd.DataFrame(new_records)
                
                # Merge with dimensions
                if not st.session_state.dim_location.empty and "location_id" in df_new.columns:
                    df_new = df_new.merge(st.session_state.dim_location, on="location_id", how="left", suffixes=('', '_loc'))
                
                if not st.session_state.dim_time.empty and "time_id" in df_new.columns:
                    df_new = df_new.merge(st.session_state.dim_time, on="time_id", how="left", suffixes=('', '_time'))
                
                if not st.session_state.dim_event.empty and "event_id" in df_new.columns:
                    df_new = df_new.merge(st.session_state.dim_event, on="event_id", how="left", suffixes=('', '_event'))
                
                if not st.session_state.dim_model.empty and "model_id" in df_new.columns:
                    df_new = df_new.merge(st.session_state.dim_model, on="model_id", how="left", suffixes=('', '_model'))
                
                # Append to existing data
                st.session_state.all_data = pd.concat([st.session_state.all_data, df_new], ignore_index=True)
            
            time.sleep(0.5)
            st.rerun()
            
        except Exception as e:
            st.error(f"Streaming error: {str(e)}")
            st.session_state.is_streaming = False


# ============================================================
#     MAIN ANALYTICS DASHBOARD
# ============================================================
if st.session_state.all_data.empty:
    st.info("👆 Click 'Load ALL Historical Data' to start analyzing your weather data")
    st.info("💡 Then you can enable real-time streaming to see live updates")
else:
    df = st.session_state.all_data.copy()
    
    # Fill missing values
    df.fillna({
        "Weather_condition": "Unknown",
        "country": "Unknown",
        "city": "Unknown",
        "season": "Unknown",
        "model_name": "Unknown"
    }, inplace=True)
    
    # ============================================================
    #     TAB-based Analytics
    # ============================================================
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview", 
        "🌡️ Temperature Analysis", 
        "☁️ Weather Patterns",
        "📍 Geographic Analysis",
        "📋 Raw Data"
    ])
    
    # ============================================================
    #     TAB 1: Overview
    # ============================================================
    with tab1:
        st.header("📊 Overall Statistics")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("📊 Total Records", f"{len(df):,}")
        
        if 'temperature' in df.columns:
            col2.metric("🌡️ Avg Temp", f"{df['temperature'].mean():.2f}°C")
            col3.metric("🔥 Max Temp", f"{df['temperature'].max():.2f}°C")
        
        if 'humidity' in df.columns:
            col4.metric("💧 Avg Humidity", f"{df['humidity'].mean():.2f}%")
        
        if 'wind_speed' in df.columns:
            col5.metric("💨 Avg Wind", f"{df['wind_speed'].mean():.2f} km/h")
        
        st.divider()
        
        # Time series charts
        col_left, col_right = st.columns(2)
        
        with col_left:
            if 'temperature' in df.columns:
                st.subheader("🌡️ Temperature Over Time")
                fig = px.line(df.reset_index(), y='temperature', 
                             title='Temperature Trend',
                             labels={'index': 'Record #', 'temperature': 'Temperature (°C)'})
                fig.update_traces(line_color='#FF6B6B')
                st.plotly_chart(fig, use_container_width=True)
        
        with col_right:
            if 'humidity' in df.columns:
                st.subheader("💧 Humidity Over Time")
                fig = px.line(df.reset_index(), y='humidity',
                             title='Humidity Trend',
                             labels={'index': 'Record #', 'humidity': 'Humidity (%)'})
                fig.update_traces(line_color='#4ECDC4')
                st.plotly_chart(fig, use_container_width=True)
        
        # Weather distribution
        if 'Weather_condition' in df.columns:
            st.subheader("☁️ Weather Conditions Distribution")
            weather_counts = df['Weather_condition'].value_counts()
            fig = px.pie(values=weather_counts.values, names=weather_counts.index,
                        title='Weather Conditions')
            st.plotly_chart(fig, use_container_width=True)
    
    # ============================================================
    #     TAB 2: Temperature Analysis
    # ============================================================
    with tab2:
        st.header("🌡️ Temperature Deep Dive")
        
        if 'temperature' in df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Temperature distribution
                st.subheader("📊 Temperature Distribution")
                fig = px.histogram(df, x='temperature', nbins=50,
                                  title='Temperature Distribution',
                                  labels={'temperature': 'Temperature (°C)'})
                st.plotly_chart(fig, use_container_width=True)
                
                # Temperature by season
                if 'season' in df.columns:
                    st.subheader("🍂 Temperature by Season")
                    fig = px.box(df, x='season', y='temperature',
                                title='Temperature Distribution by Season',
                                labels={'temperature': 'Temperature (°C)', 'season': 'Season'})
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Temperature statistics by city
                if 'city' in df.columns:
                    st.subheader("📍 Temperature by City")
                    city_temps = df.groupby('city')['temperature'].agg(['mean', 'min', 'max']).reset_index()
                    city_temps = city_temps.nlargest(10, 'mean')
                    
                    fig = go.Figure()
                    fig.add_trace(go.Bar(name='Average', x=city_temps['city'], y=city_temps['mean']))
                    fig.update_layout(title='Top 10 Cities by Average Temperature')
                    st.plotly_chart(fig, use_container_width=True)
                
                # Temperature vs Humidity scatter
                if 'humidity' in df.columns:
                    st.subheader("🌡️💧 Temperature vs Humidity")
                    sample_df = df.sample(min(1000, len(df)))
                    fig = px.scatter(sample_df, x='temperature', y='humidity',
                                    title='Temperature vs Humidity Correlation',
                                    labels={'temperature': 'Temperature (°C)', 'humidity': 'Humidity (%)'})
                    st.plotly_chart(fig, use_container_width=True)
    
    # ============================================================
    #     TAB 3: Weather Patterns
    # ============================================================
    with tab3:
        st.header("☁️ Weather Patterns Analysis")
        
        if 'Weather_condition' in df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top weather conditions
                st.subheader("🏆 Top Weather Conditions")
                weather_counts = df['Weather_condition'].value_counts().head(10)
                fig = px.bar(x=weather_counts.index, y=weather_counts.values,
                            title='Most Common Weather Conditions',
                            labels={'x': 'Weather Condition', 'y': 'Count'})
                st.plotly_chart(fig, use_container_width=True)
                
                # Weather by season
                if 'season' in df.columns:
                    st.subheader("🍂 Weather by Season")
                    season_weather = df.groupby(['season', 'Weather_condition']).size().reset_index(name='count')
                    fig = px.bar(season_weather, x='season', y='count', color='Weather_condition',
                                title='Weather Conditions by Season')
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Weather condition metrics
                st.subheader("📊 Metrics by Weather Condition")
                if 'temperature' in df.columns:
                    weather_stats = df.groupby('Weather_condition').agg({
                        'temperature': 'mean',
                        'humidity': 'mean' if 'humidity' in df.columns else lambda x: 0,
                        'wind_speed': 'mean' if 'wind_speed' in df.columns else lambda x: 0
                    }).reset_index()
                    
                    weather_stats = weather_stats.nlargest(10, 'temperature')
                    
                    fig = make_subplots(rows=3, cols=1, 
                                       subplot_titles=('Avg Temperature', 'Avg Humidity', 'Avg Wind Speed'))
                    
                    fig.add_trace(go.Bar(x=weather_stats['Weather_condition'], 
                                        y=weather_stats['temperature'], name='Temp'),
                                 row=1, col=1)
                    
                    if 'humidity' in df.columns:
                        fig.add_trace(go.Bar(x=weather_stats['Weather_condition'], 
                                            y=weather_stats['humidity'], name='Humidity'),
                                     row=2, col=1)
                    
                    if 'wind_speed' in df.columns:
                        fig.add_trace(go.Bar(x=weather_stats['Weather_condition'], 
                                            y=weather_stats['wind_speed'], name='Wind'),
                                     row=3, col=1)
                    
                    fig.update_layout(height=800, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
    
    # ============================================================
    #     TAB 4: Geographic Analysis
    # ============================================================
    with tab4:
        st.header("📍 Geographic Analysis")
        
        if 'city' in df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top cities by observations
                st.subheader("🏙️ Top Cities by Observations")
                city_counts = df['city'].value_counts().head(15)
                fig = px.bar(x=city_counts.index, y=city_counts.values,
                            title='Most Observed Cities',
                            labels={'x': 'City', 'y': 'Number of Observations'})
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Country distribution
                if 'country' in df.columns:
                    st.subheader("🌍 Observations by Country")
                    country_counts = df['country'].value_counts().head(10)
                    fig = px.pie(values=country_counts.values, names=country_counts.index,
                                title='Distribution by Country')
                    st.plotly_chart(fig, use_container_width=True)
            
            # City comparison table
            st.subheader("📊 City Statistics Comparison")
            city_stats = df.groupby('city').agg({
                'temperature': ['mean', 'min', 'max'] if 'temperature' in df.columns else lambda x: 0,
                'humidity': 'mean' if 'humidity' in df.columns else lambda x: 0,
                'wind_speed': 'mean' if 'wind_speed' in df.columns else lambda x: 0,
                'city': 'count'
            }).reset_index()
            
            city_stats.columns = ['City', 'Avg Temp', 'Min Temp', 'Max Temp', 'Avg Humidity', 'Avg Wind', 'Observations']
            city_stats = city_stats.nlargest(20, 'Observations')
            st.dataframe(city_stats, use_container_width=True, height=400)
    
    # ============================================================
    #     TAB 5: Raw Data
    # ============================================================
    with tab5:
        st.header("📋 Raw Data Explorer")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'city' in df.columns:
                cities = ['All'] + sorted(df['city'].unique().tolist())
                selected_city = st.selectbox("Filter by City", cities)
        
        with col2:
            if 'Weather_condition' in df.columns:
                conditions = ['All'] + sorted(df['Weather_condition'].unique().tolist())
                selected_condition = st.selectbox("Filter by Weather", conditions)
        
        with col3:
            if 'season' in df.columns:
                seasons = ['All'] + sorted(df['season'].unique().tolist())
                selected_season = st.selectbox("Filter by Season", seasons)
        
        # Apply filters
        filtered_df = df.copy()
        if 'city' in df.columns and selected_city != 'All':
            filtered_df = filtered_df[filtered_df['city'] == selected_city]
        if 'Weather_condition' in df.columns and selected_condition != 'All':
            filtered_df = filtered_df[filtered_df['Weather_condition'] == selected_condition]
        if 'season' in df.columns and selected_season != 'All':
            filtered_df = filtered_df[filtered_df['season'] == selected_season]
        
        st.info(f"Showing {len(filtered_df):,} records out of {len(df):,} total")
        
        # Display data
        st.dataframe(filtered_df, use_container_width=True, height=600)
        
        # Download button
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )