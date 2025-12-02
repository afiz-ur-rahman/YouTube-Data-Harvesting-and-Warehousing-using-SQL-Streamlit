# app.py
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import mysql.connector
import isodate  # for parsing YouTube ISO 8601 durations

# -----------------------------
# CONFIGURATION
# -----------------------------
API_KEY = "AIzaSyAl8ZY4UZQ0giyVLc83Envgr6hn-9T_Uu4"  # Replace with your key

DB_HOST = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"
DB_PORT = 4000
DB_USER = "bhnPubxKqYFe1KQ.root"
DB_PASSWORD = "QD8d4nxvg0L5iSJ9"
DB_NAME = "youtube"

# -----------------------------
# CONNECT TO YOUTUBE API
# -----------------------------
youtube = build('youtube', 'v3', developerKey=API_KEY)

# -----------------------------
# CONNECT TO MYSQL
# -----------------------------
conn = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)
cursor = conn.cursor(dictionary=True)

# -----------------------------
# CREATE TABLES + FIX MISSING COLUMNS
# -----------------------------
# Channels Table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Channels (
    channel_id VARCHAR(50) PRIMARY KEY,
    channel_name VARCHAR(255),
    subscribers INT,
    total_views BIGINT
)
""")

# Videos Table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Videos (
    video_id VARCHAR(50) PRIMARY KEY,
    channel_id VARCHAR(50),
    title VARCHAR(255),
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    published_at DATETIME,
    duration INT,
    FOREIGN KEY (channel_id) REFERENCES Channels(channel_id)
)
""")
conn.commit()

# -----------------------------
# FUNCTIONS
# -----------------------------
def get_channel_data(channel_id):
    res = youtube.channels().list(part='snippet,statistics', id=channel_id).execute()
    data = res['items'][0]
    return {
        'channel_id': data['id'],
        'channel_name': data['snippet']['title'],
        'subscribers': int(data['statistics'].get('subscriberCount', 0)),
        'total_views': int(data['statistics'].get('viewCount', 0))
    }

def get_videos(channel_id, max_results=50):
    res = youtube.search().list(
        part='id',
        channelId=channel_id,
        maxResults=max_results,
        type='video'
    ).execute()

    videos = []
    for item in res['items']:
        video_id = item['id']['videoId']
        stats = youtube.videos().list(part='snippet,statistics,contentDetails', id=video_id).execute()
        v = stats['items'][0]
        # Parse duration to seconds
        duration_seconds = isodate.parse_duration(v['contentDetails']['duration']).total_seconds()
        videos.append({
            'video_id': v['id'],
            'channel_id': channel_id,
            'title': v['snippet']['title'],
            'views': int(v['statistics'].get('viewCount', 0)),
            'likes': int(v['statistics'].get('likeCount', 0)),
            'comments': int(v['statistics'].get('commentCount', 0)),
            'published_at': v['snippet']['publishedAt'],
            'duration': int(duration_seconds)
        })
    return videos

def save_to_db(channel_id):
    # Save channel
    ch_data = get_channel_data(channel_id)
    cursor.execute("""
        INSERT INTO Channels (channel_id, channel_name, subscribers, total_views) 
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            channel_name=%s, 
            subscribers=%s, 
            total_views=%s
    """, (
        ch_data['channel_id'], ch_data['channel_name'], ch_data['subscribers'], ch_data['total_views'],
        ch_data['channel_name'], ch_data['subscribers'], ch_data['total_views']
    ))
    conn.commit()

    # Save videos
    videos = get_videos(channel_id)
    for v in videos:
        cursor.execute("""
            INSERT INTO Videos (video_id, channel_id, title, views, likes, comments, published_at, duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                title=%s, views=%s, likes=%s, comments=%s, published_at=%s, duration=%s
        """, (
            v['video_id'], v['channel_id'], v['title'], v['views'], v['likes'], v['comments'], v['published_at'], v['duration'],
            v['title'], v['views'], v['likes'], v['comments'], v['published_at'], v['duration']
        ))
    conn.commit()

# -----------------------------
# STREAMLIT UI
# -----------------------------
st.title("YouTube Data Harvesting & Warehousing using sql and streamlit")

st.write("Enter multiple YouTube Channel IDs separated by commas or new lines:")

channel_ids_input = st.text_area("Channel IDs:")

def parse_ids(raw):
    if not raw.strip():
        return []
    cleaned = [c.strip() for c in raw.replace("\n", ",").split(",")]
    return [c for c in cleaned if c]

channel_ids = parse_ids(channel_ids_input)

# -----------------------------
# Fetch & Save Data
# -----------------------------
if st.button("Fetch & Save Data"):
    if channel_ids:
        for cid in channel_ids:
            try:
                save_to_db(cid)
                st.success(f"✔ Data saved for Channel ID: {cid}")
            except Exception as e:
                st.error(f" Error saving {cid}: {str(e)}")
    else:
        st.warning("Please enter at least one channel ID.")

# -----------------------------
# Display Saved Data
# -----------------------------
if st.button("Show Channel & Video Data"):
    if channel_ids:
        for cid in channel_ids:
            st.subheader(f"Channel ID: {cid}")

            cursor.execute("SELECT * FROM Channels WHERE channel_id=%s", (cid,))
            channel = cursor.fetchone()
            if channel:
                st.json(channel)
            else:
                st.warning(f"No channel info found for {cid}.")
                continue

            cursor.execute(
                "SELECT title, views, likes, comments, published_at, duration FROM Videos WHERE channel_id=%s",
                (cid,)
            )
            videos = cursor.fetchall()
            if videos:
                st.dataframe(pd.DataFrame(videos))
            else:
                st.info(f"No videos found for {cid}.")
    else:
        st.warning("Please enter at least one channel ID.")

# -----------------------------
# SQL Queries for Analytics
# -----------------------------
st.subheader("Analytics Queries")

query_mapping = {
    "All Videos & Their Channels": """
        SELECT v.title AS video_name, c.channel_name
        FROM Videos v
        JOIN Channels c ON v.channel_id = c.channel_id;
    """,
    "Channels with Most Videos": """
        SELECT c.channel_name, COUNT(v.video_id) AS video_count
        FROM Videos v
        JOIN Channels c ON v.channel_id = c.channel_id
        GROUP BY c.channel_name
        ORDER BY video_count DESC;
    """,
    "Top 10 Most Viewed Videos": """
        SELECT v.title AS video_name, c.channel_name, v.views
        FROM Videos v
        JOIN Channels c ON v.channel_id = c.channel_id
        ORDER BY v.views DESC
        LIMIT 10;
    """,
    "Comments per Video": """
        SELECT v.title AS video_name, v.comments
        FROM Videos v;
    """,
    "Videos with Highest Likes": """
        SELECT v.title AS video_name, c.channel_name, v.likes
        FROM Videos v
        JOIN Channels c ON v.channel_id = c.channel_id
        ORDER BY v.likes DESC
        LIMIT 10;
    """,
    "Total Likes per Video": """
        SELECT title AS video_name, likes
        FROM Videos;
    """,
    "Total Views per Channel": """
        SELECT c.channel_name, SUM(v.views) AS total_views
        FROM Videos v
        JOIN Channels c ON v.channel_id = c.channel_id
        GROUP BY c.channel_name;
    """,
    "Channels with Videos in 2022": """
        SELECT DISTINCT c.channel_name
        FROM Videos v
        JOIN Channels c ON v.channel_id = c.channel_id
        WHERE YEAR(v.published_at) = 2022;
    """,
    "Average Duration per Channel": """
        SELECT c.channel_name, AVG(v.duration) AS avg_duration_seconds
        FROM Videos v
        JOIN Channels c ON v.channel_id = c.channel_id
        GROUP BY c.channel_name;
    """,
    "Videos with Highest Comments": """
        SELECT v.title AS video_name, c.channel_name, v.comments
        FROM Videos v
        JOIN Channels c ON v.channel_id = c.channel_id
        ORDER BY v.comments DESC
        LIMIT 10;
    """
}

for label, sql in query_mapping.items():
    if st.button(label):
        cursor.execute(sql)
        result = cursor.fetchall()
        if result:
            st.dataframe(pd.DataFrame(result))
        else:
            st.info("No data found.")




