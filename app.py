import streamlit as st
import pandas as pd
import time
import matplotlib.pyplot as plt
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re

# Initialize Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Function to clean comment text
def clean_text(text):
    text = re.sub(r'http\S+', '', text)  # Remove URLs
    text = re.sub(r'[^A-Za-z0-9 ]+', '', text)  # Remove special characters
    return text.strip()

# Function to analyze sentiment using VADER
def get_sentiment(text):
    sentiment_score = analyzer.polarity_scores(text)["compound"]
    return 1 if sentiment_score > 0 else -1  # 1 for Bullish, -1 for Bearish

# Streamlit UI
st.title("📈 Real-Time WallStreetBets Sentiment Analysis")

# Load and process data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    df = pd.read_csv("comments.csv")
    df.columns = ["comment_id", "post_title", "comment", "created_utc"]  # Ensure correct column names
    
    # Convert timestamp to datetime
    df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s")
    
    # Clean comment text
    df["comment"] = df["comment"].apply(clean_text)
    
    # Assign sentiment scores (+1 for Bullish, -1 for Bearish)
    df["sentiment_score"] = df["comment"].apply(get_sentiment)
    
    return df

df = load_data()

# Display latest sentiment updates
st.subheader("🔍 Live Sentiment Updates")
st.dataframe(df.tail(10))

# Show sentiment statistics
st.subheader("📊 Market Sentiment Overview")
total_comments = len(df)
bullish_count = (df["sentiment_score"] == 1).sum()
bearish_count = (df["sentiment_score"] == -1).sum()
avg_sentiment = df["sentiment_score"].mean()

st.write(f"💬 **Total Comments Analyzed:** {total_comments}")
st.write(f"🐂 **Bullish Comments:** {bullish_count}  |  🐻 **Bearish Comments:** {bearish_count}")
st.write(f"📊 **Average Sentiment Score:** {round(avg_sentiment, 2)} (Positive means bullish, Negative means bearish)")

# Aggregate sentiment score over time
df_time = df.resample("5min", on="created_utc").sum()

# Calculate Moving Average for Smoother Trends
df_time["sentiment_ma"] = df_time["sentiment_score"].rolling(window=3).mean()

# Plot sentiment trend with labels
st.subheader("📈 Sentiment Trend Over Time")
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(df_time.index, df_time["sentiment_score"], label="Sentiment Score", marker='o', linestyle='-', color='blue')
ax.plot(df_time.index, df_time["sentiment_ma"], label="3-Point Moving Avg", linestyle='dashed', color='red')
ax.set_xlabel("Time (5-min intervals)")
ax.set_ylabel("Sentiment Score")
ax.set_title("Market Sentiment Over Time")
ax.legend()
ax.grid(True)

st.pyplot(fig)

# Auto-refresh every 5 minutes
time.sleep(300)
st.rerun()
