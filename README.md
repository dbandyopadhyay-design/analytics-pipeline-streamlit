# 🚀 Automated Analytics Pipeline

A comprehensive 5-step analytics pipeline for social media data analysis with AI-powered insights.

## Features

- **🔍 5-Step Automated Pipeline**: Table Check → Impressions → Tagging → Analytics → AI Insights → HTML Report
- **📊 Multi-Tab Dashboard**: Overview, Metrics, Audience, Trends, Media
- **🌍 Multi-Country Support**: Analyze data from any country using ISO codes
- **📅 Flexible Date Ranges**: Rolling or specific date range selection
- **🤖 AI Summaries**: Powered by Grok AI for intelligent insights
- **📈 Interactive Charts**: Age demographics, gender distribution, daily trends
- **📥 Export Options**: CSV data and HTML dashboard downloads
- **✅ Template Validation**: Automatic validation of uploaded CSV templates

## Quick Start

1. **Upload Template**: CSV with columns: Sub Category, Terms, Handles, Hashtags, Entity ID
2. **Configure Settings**: Select country, date range, and category name
3. **Run Pipeline**: Click "Run Complete Pipeline" and wait for processing
4. **View Results**: Explore data across multiple interactive tabs
5. **Download**: Export CSV data or HTML dashboard

## Configuration

### Country Codes
Use 2-letter ISO country codes (e.g., gb, us, fr, de, es, it, ca, au, jp, br, mx, in)

### Date Modes
- **Rolling**: Last N days from yesterday (1-90 days)
- **Specific**: Custom start and end dates

### Required Template Columns
- `Sub Category`: Brand/category name
- `Terms`: Comma-separated search terms
- `Handles`: Comma-separated social media handles
- `Hashtags`: Comma-separated hashtags
- `Entity ID`: Unique identifier

## Pipeline Steps

1. **Impressions Table**: Creates/checks BigQuery table with filtered posts
2. **Template Upload**: Uploads CSV and creates tagged posts table
3. **Analytics Query**: Runs comprehensive analysis with metrics and trends
4. **AI Summaries**: Generates intelligent insights using Grok AI
5. **HTML Report**: Creates downloadable interactive dashboard

## Dashboard Tabs

- **📊 Overview**: Summary metrics and data preview
- **📈 Metrics**: Detailed performance indicators
- **👥 Audience**: Demographics, authors, and interests
- **📊 Trends**: Daily activity patterns with interactive charts
- **🎬 Media**: Top posts, photos, videos, and GIFs

## Technology Stack

- **Frontend**: Streamlit with interactive components
- **Backend**: Google BigQuery for data processing
- **AI**: OpenAI-compatible API (Grok) for summaries
- **Visualization**: Plotly for charts and analytics
- **Data**: Pandas for data manipulation

## Deployment

This app is designed to run on Streamlit Community Cloud for easy deployment and sharing.

## Requirements

See `requirements.txt` for Python dependencies.

## Author

Analytics Pipeline v2.0 - Full Featured Template