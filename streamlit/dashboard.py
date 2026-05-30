import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
import os
from datetime import datetime, timedelta

# Настройка страницы
st.set_page_config(
    page_title="University Analytics Dashboard",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS для стилизации
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 10px;
    }
    .big-font {
        font-size: 20px !important;
        font-weight: bold;
    }
    .header {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
    }
    .info-box {
        background-color: #e3f2fd;
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# Заголовок
st.markdown('<div class="header"><h1 style="color: white;">🎓 University Analytics Dashboard</h1><p style="color: white;">Semantic Layer with Cube.js | Real-time Analytics</p></div>', unsafe_allow_html=True)

# Конфигурация - из переменных окружения
CUBE_API_URL = os.environ.get("CUBE_API_URL", "http://cube:4000")
CUBE_API_TOKEN = os.environ.get("CUBE_API_TOKEN", "")

# Функции для запросов к Cube.js
@st.cache_data(ttl=60)
def query_cube(query):
    """Выполняет запрос к Cube.js API"""
    headers = {
        "Content-Type": "application/json"
    }
    
    if CUBE_API_TOKEN:
        headers["Authorization"] = CUBE_API_TOKEN
    
    try:
        response = requests.post(
            f"{CUBE_API_URL}/cubejs-api/v1/load",
            json={"query": query},
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('data'):
                return pd.DataFrame(data['data'])
        elif response.status_code == 401:
            st.warning("Authentication failed. Please check Cube.js token.")
        else:
            st.warning(f"API returned status {response.status_code}")
    except requests.exceptions.ConnectionError:
        st.warning(f"Cannot connect to Cube.js at {CUBE_API_URL}")
    except Exception as e:
        st.warning(f"Error: {e}")
    
    return pd.DataFrame()

# Проверка соединения с Cube.js
st.sidebar.markdown("## 🔍 Connection Status")

try:
    response = requests.get(f"{CUBE_API_URL}/cubejs-api/v1/meta", timeout=5)
    if response.status_code == 200:
        st.sidebar.success(f"✅ Connected to Cube.js")
    else:
        st.sidebar.warning(f"⚠️ Cube.js returned {response.status_code}")
except Exception as e:
    st.sidebar.error(f"❌ Cannot connect to Cube.js")
    st.sidebar.info(f"Make sure Cube.js is running on {CUBE_API_URL}")

st.sidebar.markdown("---")

# Фильтры
st.sidebar.markdown("## 🔍 Filters")
st.sidebar.markdown("---")

semester = st.sidebar.selectbox(
    "Semester",
    ["2026-Spring", "2025-Fall", "2025-Spring"],
    index=0
)

course_filter = st.sidebar.multiselect(
    "Courses",
    ["CS101", "CS102", "MATH201", "PHYS101", "ENG202", "HIST101"],
    default=["CS101", "CS102"]
)

st.sidebar.markdown("---")
st.sidebar.info(f"📊 API: {CUBE_API_URL}")

# Основные метрики
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("📚 Average Grade", "4.2", "↑0.3")
with col2:
    st.metric("✅ Passing Rate", "87%", "↑5%")
with col3:
    st.metric("👨‍🎓 Active Students", "1,247", "↑12")
with col4:
    st.metric("🏢 Avg Occupancy", "67%", "↓3%")

st.markdown("---")

# Графики
tab1, tab2, tab3, tab4 = st.tabs(["📈 Academic Performance", "🏛️ Campus Analytics", "📊 Course Analysis", "🎯 Drill-Down"])

with tab1:
    st.subheader("Grade Distribution by Course")
    
    # Пример данных (заглушка)
    sample_data = pd.DataFrame({
        'Course': ['CS101', 'CS102', 'MATH201', 'PHYS101', 'ENG202'],
        'Average Grade': [4.5, 4.2, 3.8, 4.0, 4.3],
        'Passing Rate': [92, 88, 75, 82, 90]
    })
    
    fig = px.bar(sample_data, x='Course', y='Average Grade',
                title="Average Grade by Course",
                color='Average Grade',
                color_continuous_scale='Viridis',
                text='Average Grade')
    fig.update_traces(textposition='outside')
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Grade Category Distribution")
    grade_dist = pd.DataFrame({
        'Category': ['Excellent', 'Good', 'Satisfactory', 'Needs Improvement'],
        'Count': [320, 450, 280, 150]
    })
    fig2 = px.pie(grade_dist, values='Count', names='Category',
                  title="Student Performance Distribution",
                  color_discrete_sequence=px.colors.qualitative.Set3)
    st.plotly_chart(fig2, use_container_width=True)

with tab2:
    st.subheader("Building Occupancy")
    occupancy_data = pd.DataFrame({
        'Building': ['Main', 'Library', 'CS Building', 'Student Center', 'Sports Complex'],
        'Avg Occupancy': [75, 85, 60, 70, 45]
    })
    fig3 = px.bar(occupancy_data, x='Building', y='Avg Occupancy',
                  title="Average Occupancy by Building",
                  color='Avg Occupancy',
                  color_continuous_scale='Hot')
    st.plotly_chart(fig3, use_container_width=True)

with tab3:
    st.subheader("Course Performance Analysis")
    st.dataframe(sample_data, use_container_width=True)
    
    fig4 = px.scatter(sample_data, x='Course', y='Average Grade',
                      size='Passing Rate', title="Course Performance Matrix")
    st.plotly_chart(fig4, use_container_width=True)

with tab4:
    st.subheader("🎯 Drill-Down Analysis")
    
    drill_level = st.radio(
        "Select Drill Level",
        ["Course → Student Details", "Building → Room Details", "Semester → Course"],
        horizontal=True
    )
    
    if drill_level == "Course → Student Details":
        selected_course = st.selectbox("Select Course", ["CS101", "CS102", "MATH201", "PHYS101", "ENG202"])
        st.info(f"📊 Showing detailed analytics for {selected_course}")
        
        # Пример детальных данных
        student_details = pd.DataFrame({
            'Student ID': [1001, 1002, 1003, 1004, 1005],
            'Grade': [4.5, 3.8, 4.2, 3.5, 4.8],
            'Attendance': [95, 82, 88, 75, 98]
        })
        st.dataframe(student_details, use_container_width=True)

# Footer
st.markdown("---")
st.markdown(f"""
<div style="text-align: center; color: #666; padding: 20px;">
    <p>🏗️ Built with Cube.js Semantic Layer + Streamlit | Data Pipeline: Bronze → Silver → Gold</p>
    <p>📊 Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    <p>🔗 <a href="{CUBE_API_URL}" target="_blank">Cube.js Playground</a></p>
</div>
""", unsafe_allow_html=True)
