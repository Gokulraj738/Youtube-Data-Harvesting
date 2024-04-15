import streamlit as st

import mysql.connector
from googleapiclient.discovery import build
from datetime import datetime
import pandas as pd

# Connect to YouTube API
def connect_to_youtube_api(api_key):
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

# Get channel details
def get_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    )
    response = request.execute()
    channel_data = []
    for item in response.get('items',[]):
        subscriber_count = 0
        if 'subscriberCount' in item['statistics']:
            try:
                subscriber_count = int(item['statistics']['subscriberCount'])
            except ValueError:
                pass
        channel_id = item.get('id', '')
        channel_name = item['snippet'].get('title', '')
        channel_description = item['snippet'].get('description', '')
        view_count = int(item['statistics'].get('viewCount', 0))
        video_count = int(item['statistics'].get('videoCount', 0))
        playlists = item['contentDetails'].get('relatedPlaylists', {})
        uploads_playlist_id = playlists.get('uploads', '')

        channel_Details = {
            'channel_id': channel_id,
            'channel_name': channel_name,
            'subscriber_count': subscriber_count,
            'view_count': view_count,
            'video_count': video_count,
            'channel_description': channel_description,
            'playLists_id': uploads_playlist_id
        }
        channel_data.append(channel_Details)
    return channel_data

# Get video details
def get_video_details(youtube, channel_id):
    video_ids = []
    try:
        response = youtube.channels().list(
            id=channel_id,
            part='contentDetails'
        ).execute()
        playlists_id = response['items'][0].get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads', None)

        if playlists_id is None:
            # Handle case when 'contentDetails' or 'relatedPlaylists' or 'uploads' is missing
            return video_ids

        nextpagetoken = None
        while True:
            response1 = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlists_id,
                maxResults=50,
                pageToken=nextpagetoken
            ).execute()

            for item in response1.get('items', []):
                video_ids.append(item['snippet']['resourceId']['videoId'])
                nextpagetoken = response1.get('nextPageToken')

            if nextpagetoken is None:
                break

    except KeyError as e:
        print("KeyError:", e)
    return video_ids

# Get video information
import re
def duration_to_seconds(duration_str):
    if isinstance(duration_str, int):
        return duration_str
    # Regular expression to extract hours, minutes, and seconds
    duration_regex = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = duration_regex.match(duration_str)
    
    # Extract hours, minutes, and seconds from the matched groups
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    
    # Calculate total duration in seconds
    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds

def convert_datetime(datetime_str):
    return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
def get_video_info(youtube, video_ids):
    video_data = []
    for videoid in video_ids:
        try:
            request = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=videoid
            )
            response = request.execute()
            for item in response['items']:
                data = {
                    'Channel_Name': item['snippet']['channelTitle'],
                    'Channel_Id': item['snippet']['channelId'],
                    'Video_Id': item['id'],
                    'Title': item['snippet']['title'],
                    'Description': item['snippet']['description'],
                    'Published_Date': item['snippet']['publishedAt'],
                    'Duration': item['contentDetails']['duration'],
                    'Views': item['statistics']['viewCount'],
                    'Likes': item['statistics']['likeCount'],
                    'Comments': item['statistics'].get('commentCount', 0),
                    'Favorite_Count': item['statistics']['favoriteCount'],
                    'Definition': item['contentDetails']['definition'],
                    'Caption_Status': item['contentDetails']['caption']
                }
                video_data.append(data)
        except KeyError as e:
            print(f"KeyError while fetching video details for video ID {videoid}: {e}")
            continue  # Skip this video and continue with the next one
    return video_data

# Get comment information
def convert_datetime(datetime_str):
    return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
def get_comment_info(youtube, video_ids):
    commentData=[]
    try:
        for VIdeo_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=VIdeo_id,
                maxResults=50,
            )
            response=request.execute()
            for item in response['items']:
                Cdata=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                           Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                           CommentDisplay=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                           CommentAuthor=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                           PublishedDate=item['snippet']['topLevelComment']['snippet']['publishedAt']
                          )
            commentData.append(Cdata)
    except Exception as e:
        print("An error occurred:", e)
    return commentData  

# Store data in MySQL
def store_data_in_mysql(channel_details, video_details, comment_details):
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="CSKtsk@738",
        database="YoutubeProject"
    )
    cursor = conn.cursor()

    # Create channels table if not exists
    cursor.execute("""
        create table if not exists channels (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255),
            subscriber_count INT,
            view_count INT,
            video_count INT,
            channel_description TEXT
        )
    """)

    # Insert channel details
    for channel in channel_details:
        cursor.execute("""
            insert ignore into channels (channel_id, channel_name, subscriber_count, view_count, video_count, channel_description)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (channel['channel_id'], channel['channel_name'], channel['subscriber_count'], channel['view_count'],
          channel['video_count'], channel['channel_description']))

    # Create videos table if not exists
    cursor.execute("""
        create table if not exists videos (
            Channel_Name VARCHAR(255) ,
            Channel_Id VARCHAR(255),
            Video_Id VARCHAR(255) primary key,
            Title VARCHAR(255),
            Description TEXT,
            Published_Date DATETIME,
            Duration VARCHAR(50),
            Views INT,
            Likes INT,
            Comments INT,
            Favorite_Count INT,
            Definition VARCHAR(50),
            Caption_Status VARCHAR(50)
        )
    """)

    # Insert video details
    for video in video_details:
        cursor.execute("""
            insert ignore into videos (Channel_Name, Channel_Id, Video_Id, Title, Description, Published_Date, Duration, 
                            Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (video['Channel_Name'], video['Channel_Id'], video['Video_Id'], video['Title'], video['Description'],
          datetime.strptime(video['Published_Date'], '%Y-%m-%dT%H:%M:%SZ'), video['Duration'],
          video['Views'], video['Likes'], video['Comments'], video['Favorite_Count'],
          video['Definition'], video['Caption_Status']))

    # Create comments table if not exists
    cursor.execute("""
        create table if not exists comments (
            Comment_Id VARCHAR(255) PRIMARY KEY,
            Video_Id VARCHAR(255),
            CommentDisplay TEXT,
            CommentAuthor VARCHAR(255),
            PublishedDate DATETIME
        )
    """)

    # Insert comment details
    for comment in comment_details:
        cursor.execute("""
            insert ignore into comments (Comment_Id, Video_Id, CommentDisplay, CommentAuthor, PublishedDate)
            VALUES (%s, %s, %s, %s, %s)
        """, (comment['Comment_Id'], comment['Video_Id'], comment['CommentDisplay'],
              comment['CommentAuthor'], datetime.strptime(comment['PublishedDate'], '%Y-%m-%dT%H:%M:%SZ')))

    conn.commit()
    cursor.close()
    conn.close()


def fetch_data_from_mysql(query):
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="CSKtsk@738",
        database="YoutubeProject"
    )
    cursor = conn.cursor()
    # Fetch channel details
    cursor.execute("SELECT * FROM channels")
    channel_data = cursor.fetchall()
    channel_columns = [desc[0] for desc in cursor.description]
    channel_df = pd.DataFrame(channel_data, columns=channel_columns)

    # Fetch video details
    cursor.execute("SELECT * FROM videos")
    video_data = cursor.fetchall()
    video_columns = [desc[0] for desc in cursor.description]
    video_df = pd.DataFrame(video_data, columns=video_columns)

    # Fetch comment details
    cursor.execute("SELECT * FROM comments")
    comment_data = cursor.fetchall()
    comment_columns = [desc[0] for desc in cursor.description]
    comment_df = pd.DataFrame(comment_data, columns=comment_columns)

    conn.commit()
    cursor.close()
    conn.close()

    return channel_df, video_df, comment_df


# Main function to fetch data for 10 channels and store in MySQL
def execute_query(query):
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="CSKtsk@738",
        database="YoutubeProject"
    )
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def main():
    with st.sidebar:
        st.title("YouTube Data Collection and Display")

        # YouTube API key input
        api_key = st.text_input("Enter your YouTube API key")

    # Channel ID input
    channel_id = st.text_input("Enter the YouTube channel ID")

    if st.button("Collect and store data"):
        if not api_key:
            st.error("Please enter your YouTube API key")
        elif not channel_id:
            st.error("Please enter a YouTube channel ID")
        else:
            youtube = connect_to_youtube_api(api_key)
            channel_details = get_channel_details(youtube, channel_id)
            video_ids = get_video_details(youtube, channel_id)
            video_details = get_video_info(youtube, video_ids)
            comment_details = get_comment_info(youtube, video_ids)
            store_data_in_mysql(channel_details, video_details, comment_details)
            st.success("Data collected and stored successfully")

    channel_df, video_df, comment_df = fetch_data_from_mysql('')


    # Display data
    if st.checkbox("Show Channel Details"):
        st.subheader("Channel Details")
        st.write(channel_df)

    if st.checkbox("Show Video Details"):
        st.subheader("Video Details")
        st.write(video_df)

    if st.checkbox("Show Comment Details"):
        st.subheader("Comment Details")
        st.write(comment_df)


    # Connect to MySQL database
    mydb = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="CSKtsk@738",
    database="YoutubeProject"
)
    cursor = mydb.cursor()

    # Select the question
    question = st.selectbox("Select your question", [
        "1. All the videos and the channel name",
        "2. Channels with most number of videos",
        "3. 10 most viewed videos",
        "4. Comments in each video",
        "5. Videos with highest likes",
        "6. Likes of all videos",
        "7. Views of each channel",
        "8. Videos published in the year of 2022",
        "9. Average duration of all videos in each channel",
        "10. Videos with highest number of comments"
    ])

    # Generate SQL query based on the selected question

    if question.startswith("1. All the videos and the channel name"):
        query = "SELECT v.title AS video_title, c.channel_name FROM videos v JOIN channels c ON v.channel_id = c.channel_id"
    elif question.startswith("2. Channels with most number of videos"):
        query = "SELECT c.channel_name AS channelname, c.video_count AS no_videos FROM channels c ORDER BY c.video_count DESC"
    elif question.startswith("3. 10 most viewed videos"):
        query = "SELECT v.views AS views, c.channel_name AS channelname, v.title AS videotitle FROM videos v JOIN channels c ON v.channel_id = c.channel_id WHERE v.views IS NOT NULL ORDER BY v.views DESC LIMIT 10"
    elif question.startswith("4. Comments in each video"):
        query = "SELECT v.comments AS no_comments, v.title AS videotitle FROM videos v WHERE v.comments IS NOT NULL"
    elif question.startswith("5. Videos with highest likes"):
        query = "SELECT v.title AS videotitle, c.channel_name AS channelname, v.likes AS likecount FROM videos v JOIN channels c ON v.channel_id = c.channel_id WHERE v.likes IS NOT NULL ORDER BY v.likes DESC"
    elif question.startswith("6. Likes of all videos"):
        query = "SELECT v.likes AS likecount, v.title AS videotitle FROM videos v"
    elif question.startswith("7. Views of each channel"):
        query = "SELECT c.channel_name AS channelname, c.view_count AS totalviews FROM channels c"
    elif question.startswith("8. Videos published in the year of 2022"):
        query = "SELECT v.title AS video_title, v.published_date AS videorelease, c.channel_name AS channelname FROM videos v JOIN channels c ON v.channel_id = c.channel_id WHERE EXTRACT(YEAR FROM v.published_date) = 2022"
    elif question.startswith("9. Average duration of all videos in each channel"):
        query = "SELECT c.channel_name AS channelname, AVG(v.duration) AS averageduration FROM videos v JOIN channels c ON v.channel_id = c.channel_id GROUP BY c.channel_name"
    elif question.startswith("10. Videos with highest number of comments"):
        query = "SELECT v.title AS videotitle, c.channel_name AS channelname, v.comments AS comments FROM videos v JOIN channels c ON v.channel_id = c.channel_id WHERE v.comments IS NOT NULL ORDER BY v.comments DESC"


    # Fetch data when button is clicked
    fetch_button_key = "fetch_data_button"

    if st.button("Fetch Data", key=fetch_button_key):
        if query:
            # Execute SQL query and fetch data
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=[col[0] for col in cursor.description])
            st.write(df)
        else:
            st.error("Please select a question")

    # Close MySQL connection
    cursor.close()
    mydb.close()

if __name__ == "__main__":
    main()