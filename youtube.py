# main code Starts
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import psycopg2
from psycopg2 import Error
import streamlit as st
import requests

# If using new python package  then run the following commands before the above code cell 
# or if not installed packages then install it by running following commands
# !pip install streamlit
# !pip install google-api-python-client
# !pip install --upgrade google-api-python-client

# Stylized stream title generator function

# def generate_stylish_title(title):
#     stylish_title = title.upper()  # Convert title to uppercase for emphasis
#     stylish_title = " ".join([word.capitalize() for word in stylish_title.split()])  # Capitalize each word
#     stylish_title = "🌟 " + stylish_title + " 🌟"  # Add star emojis for flair
#     return stylish_title

# Example usage
stream_title = "Youtube Data Harvesting"
# stylized_title = generate_stylish_title(stream_title)
st.header(stream_title, divider='rainbow')


# st.header("Youtube Data Harvesting", anchor=True)
api_key1 = 'AIzaSyDKHambPDM9ZrMk0TzeWMMRpsf_j8UAK_k'
api_key2 = 'AIzaSyDgv4jXqjXTY5Usng5Vln0tZM1CH6pcGww'
# Api Key
def API_connect(api_key):
    
    
    api_service_name = 'youtube'
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=api_key)

    return youtube

youtube = API_connect(api_key2)

# get channel information
def get_channel_info(channel_id):
    response = youtube.channels().list(
        id = channel_id,
        part = 'snippet,statistics,contentDetails'
    )

    channel1 = response.execute()

    for i in channel1['items']:
        channel_data = {"Channel_Id" : i['id'],
            "Channel_name" : i['snippet']['title'],
            "Subscribers"  : i['statistics']['subscriberCount'],
            "Channel_Description" : i['snippet']['description'],
            "Total_views" :  i['statistics']['viewCount'],
            "Total_videos" : i['statistics']['videoCount'],
            "Playlist_id" : i['contentDetails']['relatedPlaylists']['uploads']
        }
    return channel_data

# get videos  Id
def get_all_videoids(channel_id):
    video_ids = []
    response  = youtube.channels().list(
        id = channel_id,
        part = 'snippet,statistics,contentDetails'
    ).execute()

    playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    
    while True:
        playlist_details = youtube.playlistItems().list(
                playlistId = playlist_ID,
                part = 'snippet', maxResults = 50,
                pageToken = next_page_token
            ).execute()
        for i in playlist_details['items']:
            video_ids.append(i['snippet']['resourceId']['videoId'])
        next_page_token = playlist_details.get('nextPageToken')

        if next_page_token is None:
    
            break
    return video_ids
    
# get video details
def get_video_info(videoids):
    video_data = []

    for videoid in videoids:
        response =  youtube.videos().list(
                id = videoid,
                part = 'snippet,statistics,contentDetails'
            ).execute()

        for i in response['items']:
            # Sometimes a particular key is not available for example commentcount key is not available 
            # for videos where there are no comments to handel that issue
            # we can use get() to specify a default value(here 0) if the key is not found
            video_details = {"Video_id" : i['id'],
                            "Channel_Name" : i['snippet']['channelTitle'],
                        'Video_Name' : i['snippet']['title'],
                    "Video_Description" : i['snippet']['description'],
                    "Tags" : i['snippet'].get('tags', None),
                    "PublishedAt" : i['snippet']['publishedAt'],
                    "View_Count" : i['statistics'].get('viewCount', 0),
                    "Like_Count" : i['statistics'].get('likeCount', 0),
                    "Favorite_Count" : i['statistics'].get('favoriteCount', 0),
                    "Comment_Count" : i['statistics'].get('commentCount', 0),
                    "Duration" : i['contentDetails']['duration'],
                    "Thumbnails" : i['snippet']['thumbnails']['medium']['url'],
                    "Caption_Status" : i['contentDetails']['caption']

            }
        video_data.append(video_details)
    return video_data

# get comment info

def get_comment_data_info(videoids):
    videos_comments_data = []
    for video_id in videoids:
        try:
            request =  youtube.commentThreads().list(
                videoId = video_id,
                part = 'snippet',
                maxResults = 50
            ).execute()
        except HttpError as e:
                error_reason = e.error_details[0]['reason']
                if error_reason == 'commentsDisabled':
                    print(f"Comments are disabled for the video with ID: {video_id}. Skipping...")

        for i in request['items']:
            data = { 'Video_Id' : i['snippet']['videoId'],
                    "Comment_ID" : i['snippet']['topLevelComment']['id'],
                    'Comment_text' : i['snippet']['topLevelComment']['snippet']['textOriginal'],
                    "Comment_Author" : i['snippet']['topLevelComment']['snippet']["authorDisplayName"],
                    "Comment_Published" : i['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
            videos_comments_data.append(data)

    return videos_comments_data

# get playlist details

def get_playlist_info(channel_id):

    playlist_info = []
    next_page_token = None

    while True:
        request = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token,
            
        ).execute()

        for i in request['items']:
            data = {
                "Playlist_Id" : i['id'],
                "Playlist_title" : i["snippet"]['title'],
                "Channel_Id" : i["snippet"]['channelId'],
                "Channel_Name" : i['snippet']['channelTitle'],
                "PublishedAt" : i['snippet']['publishedAt'],
                "Video_count" : i['contentDetails']['itemCount']
            }
            playlist_info.append(data)
        next_page_token = request.get('nextPageToken')
        if next_page_token == None:
            break
        
    return playlist_info

# Create channel table
def create_channels_table(channel_ids):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Youtube_Data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists channels(Channel_Id varchar(80) primary key,
                                                        Channel_name varchar(100),
                                                        Subscribers bigint,
                                                        Channel_Description text,
                                                        Total_views  bigint,
                                                        Total_videos int,
                                                        Playlist_id varchar(80))'''

    cursor.execute(create_query)
    mydb.commit()
    print("Channels table created")

    # Insert channel information into SQL table
    def insert_channel_info(mydb, channel_details):
        cursor = mydb.cursor()
        try:
            # Define the SQL query to insert data into the Channel table
            sql_query = """INSERT INTO channels(Channel_Id, Channel_name, Subscribers, Channel_Description, Total_views, Total_videos, Playlist_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)"""

            # Execute the SQL query
            cursor.execute(sql_query, (channel_details['Channel_Id'], channel_details['Channel_name'], channel_details['Subscribers'], channel_details['Channel_Description'], channel_details['Total_views'], channel_details['Total_videos'], channel_details['Playlist_id']))

            # Commit the transaction
            mydb.commit()
            # print("Record inserted successfully into Channel table")
            print(f"Record inserted successfully for channel ID: {channel_details['Channel_Id']}")
        except (Exception, Error) as error:
            print("Error while inserting record into Channel table", error)


    # Iterate over channel IDs, fetch details, and insert into SQL table
    def fetch_and_insert_channel_info(mydb):
        global ch_df
        ch_df = pd.DataFrame()
        if mydb:
            for channel_id in channel_ids:
                channel_info = get_channel_info(channel_id)
                ch_df = ch_df._append(channel_info, ignore_index = True)
                insert_channel_info(mydb, channel_info)
            

    # Call the function to fetch and insert channel information
    fetch_and_insert_channel_info(mydb)
    return ch_df

# Create videos table
def create_videos_table(channel_ids):    
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Youtube_Data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    # changed duration int to interval due to values are given in PT4M25S format
    try:
        create_query='''create table if not exists videos(Video_id varchar(255) primary key,
                                                            Channel_Name varchar(255),
                                                            Video_Name varchar(255),
                                                            Video_Description text,
                                                            Tags text,
                                                            PublishedAt timestamp,
                                                            View_Count int,
                                                            Like_Count int,
                                                            Favorite_Count int,
                                                            Comment_Count int,
                                                            Duration interval, 
                                                            Thumbnails varchar(255),
                                                            Caption_Status varchar(255)
                                                            )'''

        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channels table already created")

    def insert_video(videoids):
        try:
            for video_data in videoids:
                sql_query = """INSERT INTO videos (Video_id, Channel_Name, Video_Name, Video_Description, Tags, PublishedAt, View_Count, Like_Count, Favorite_Count, Comment_Count, Duration, Thumbnails, Caption_Status)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql_query, (
                    video_data['Video_id'],
                    video_data['Channel_Name'],
                    video_data['Video_Name'],
                    video_data['Video_Description'],
                    video_data['Tags'],
                    video_data['PublishedAt'],
                    video_data['View_Count'],
                    video_data['Like_Count'],
                    video_data['Favorite_Count'],
                    video_data['Comment_Count'],
                    video_data['Duration'],
                    video_data['Thumbnails'],
                    video_data['Caption_Status']
                ))
                mydb.commit()
                print(f"Record inserted successfully for video ID: {video_data['Video_id']}")
        except (Exception, Error) as error:
            print("Error while inserting record into videos table", error)

    def fetch_and_insert_video_info(mydb):
        global vd_df
        vd_df = pd.DataFrame()
        if mydb:
            for channel_id in channel_ids:
                videoids = get_all_videoids(channel_id)
                video_data = get_video_info(videoids)
                vd_df = vd_df._append(video_data, ignore_index = True)
                insert_video(video_data)

    # Call the function to fetch and insert channel information
    fetch_and_insert_video_info(mydb)
    return vd_df


# Create comments table
def create_comments_table(channel_ids):
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Youtube_Data",
                            port="5432")
    cursor=mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    # changed duration int to interval due to values are given in PT4M25S format

    create_query='''create table if not exists comments(Video_id varchar(255) ,
                                                    Comment_ID varchar(255) primary key,
                                                    Comment_text text,
                                                    Comment_Author varchar(255),
                                                    Comment_Published timestamp
                                                    )'''

    cursor.execute(create_query)
    mydb.commit()

    def insert_video(comment_list):
            try:
                for comment in comment_list:
                    insert_query = '''
                        INSERT INTO comments (Video_id, Comment_ID, Comment_text, Comment_Author, Comment_Published)
                        VALUES (%s, %s, %s, %s, %s)
                    '''
                    cursor.execute(insert_query,(
                                comment['Video_Id'],
                                comment['Comment_ID'],
                                comment['Comment_text'],
                                comment['Comment_Author'],
                                comment['Comment_Published']
                                ))
                    mydb.commit()
                    print(f"Record inserted successfully for comment ID: {comment['Comment_ID']}")
            except (Exception, Error) as error:
                print("Error while inserting record into comment table", error)
    def fetch_and_insert_comment_info(mydb):
        if mydb:
            global cm_df
            cm_df = pd.DataFrame()
            for channel_id in  channel_ids:
                video_ids = get_all_videoids(channel_id)
                comment_list = get_comment_data_info(video_ids)
                cm_df = cm_df._append(comment_list, ignore_index= True)
                insert_video(comment_list)
    fetch_and_insert_comment_info(mydb)

def create_playlist_table(channel_ids):
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Youtube_Data",
                        port="5432")
        cursor=mydb.cursor()

        drop_query = '''drop table if exists playlists'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query='''create table if not exists playlists(Playlist_id varchar(80) primary key,
                                                                Playlist_title varchar(255),
                                                                Channel_Id varchar(80),
                                                                Channel_name varchar(100),
                                                                PublishedAt timestamp,
                                                                Video_count  int)'''
        cursor.execute(create_query)
        mydb.commit()
        print("Channels table created")


        def insert_playlist_info(mydb, playlist_info):
                try:
                        sql_query = """INSERT INTO playlists(Playlist_id, Playlist_title, Channel_Id, Channel_name, PublishedAt, Video_count)
                                        VALUES (%s, %s, %s, %s, %s, %s)"""
                        cursor.execute(sql_query, (
                                playlist_info['Playlist_Id'],
                                playlist_info['Playlist_title'],
                                playlist_info['Channel_Id'],
                                playlist_info['Channel_Name'],
                                playlist_info['PublishedAt'],
                                playlist_info['Video_count']
                        ))
                        mydb.commit()
                        # print(f"Record inserted successfully for playlist ID: {playlist_info['Playlist_Id']}")

                except (Exception, Error) as error:
                        print("Error while inserting record into playlists table:", error)

        # Iterate over channel IDs, fetch details, and insert into SQL table
        def fetch_and_insert_playlists_info(mydb):
                print("Fetching channel ids")
                if mydb:
                        global pl_df
                        pl_df = pd.DataFrame()
                        for channel_id in channel_ids:
                                playlist_info_list = get_playlist_info(channel_id)
                                for each_playlist in playlist_info_list:
                                        pl_df = pl_df._append(each_playlist, ignore_index = True)
                                        insert_playlist_info(mydb, each_playlist)
        fetch_and_insert_playlists_info(mydb)
        return pl_df





def create_tables(channel_ids):
    create_channels_table(channel_ids)
    create_videos_table(channel_ids)
    create_comments_table(channel_ids)
    create_playlist_table(channel_ids)

    print("Tables successfully created")

# Function to get channel details
def get_channel_details(channel_id):
    # Make request to YouTube API to fetch channel details
    api_key = "AIzaSyDgv4jXqjXTY5Usng5Vln0tZM1CH6pcGww"
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet&id={channel_id}&key={api_key}"
    response = requests.get(url)
    data = response.json()

    # Extract channel name and thumbnail URL from response
    if 'items' in data and data['items']:
        snippet = data['items'][0]['snippet']
        return snippet['title'], snippet['thumbnails']['default']['url']
    else:
        return None, None


# Experiment code starts here
# Initialize session_state to store channel_ids
if 'channel_ids' not in st.session_state:
    st.session_state.channel_ids = []

def channel_id_exists(channel_id):
    # Make request to YouTube API to fetch channel details
    api_key = "AIzaSyDKHambPDM9ZrMk0TzeWMMRpsf_j8UAK_k"
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet&id={channel_id}&key={api_key}"
    response = requests.get(url)
    data = response.json()

    # Check if 'items' key exists in the response and it's not empty
    if 'items' in data and data['items']:
        return True
    else:
        return False
    
# Text input box to enter channel ID
new_channel_id = st.text_input("***Enter Channel ID:***")

# Button to add channel ID to the list
if st.button('Add Channel ID'):
    if new_channel_id:
        if channel_id_exists(new_channel_id):
            if new_channel_id not in st.session_state.channel_ids:
                st.session_state.channel_ids.append(new_channel_id)
                st.success(f"Channel ID '{new_channel_id}' added successfully!")
            else:
                st.warning(f"Channel ID '{new_channel_id}' is already present.")
        else:
            st.warning(f"Channel ID '{new_channel_id}' does not exist. Please enter a valid channel ID.")
    else:
        st.warning("Please enter a channel ID. Can't add Empty ID")

show_table=st.radio(":rainbow[AFTER ADDING CHANNEL ID SELECT FOR VIEW]",("***None***","***CHANNELS***","***VIDEOS***", "***PLAYLISTS***"))

if show_table=="***CHANNELS***":
    ch_df = create_channels_table([new_channel_id])
    st.success(f"You are viewing channel table for channel Id: {new_channel_id}")
    st.write(ch_df)


elif show_table=="***VIDEOS***":
    vd_df = create_videos_table([new_channel_id])
    st.success(f"You are viewing videos table for channel Id: {new_channel_id}")
    st.write(vd_df)


elif show_table=="***PLAYLISTS***":
    pl_df = create_playlist_table([new_channel_id])
    st.success(f"You are viewing playlists table for channel Id: {new_channel_id}")
    st.write(pl_df)



# Display the list of current channel IDs with names and thumbnails
if st.session_state.channel_ids:
    st.write("***Current Channel IDs:***")
    for channel_id in st.session_state.channel_ids:
        channel_details = get_channel_details(channel_id)
        st.write(
            f":green[Channel ID: {channel_id}]",
            f":green[Channel Name: {channel_details[0]}]",
            f":green[Thumbnail:] <img src='{channel_details[1]}' width='100'>",
            unsafe_allow_html=True
        )



# Button to initiate data retrieval
if st.button('Create Table'):
    final_channel_ids = [channel_id for channel_id in st.session_state.channel_ids]
    create_tables(final_channel_ids)
    st.success("Tables Created Succesfully!")

    
# # List of channel IDs
# # channel_ids = ["UCsXVk37bltHxD1rDPwtNM8Q"
# # , "UCgUCgVN39J_9tYiBynbVqfg"
# # , "UCrnQenczepPnuw-1ISxJ1TQ"
# # , "UCT0dmfFCLWuVKPWZ6wcdKyg"
# # ,"UCh5bICCatQ70Fx4-jwAmWKQ"] 


mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Youtube_Data",
                        port="5432")
cursor=mydb.cursor()

question=st.selectbox("***Select your question***",("1. All the videos and the channel name",
                                              "2. Channels with maximum number of videos",
                                              "3. Top 10 most viewed videos",
                                              "4. Comments in each videos",
                                              "5. Videos with highest likes",
                                              "6. Likes of all videos",
                                              "7. Views of each channel",
                                              "8. Videos published in the year of 2022",
                                              "9. Average duration of all videos in each channel",
                                              "10. Videos with highest number of comments"),
                    index = None,
                    placeholder="Select from the query below...")

if question=="1. All the videos and the channel name":
    query1='''select  video_name, channel_name from videos;'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. Channels with maximum number of videos":
    query2='''SELECT Channel_name as channelname, Total_videos as no_videos
           FROM channels
           WHERE Total_videos = (SELECT MAX(Total_videos)
                                  FROM channels)'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. Top 10 most viewed videos":
    query3='''SELECT 
                  video_name,
                  channel_name,
                  view_count
           FROM videos
           order by view_count desc
           limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["videotitle","channel name","views"])
    st.write(df3)

elif question=="4. Comments in each videos":
    query4='''select 
                  video_name as videotitle,
	              comment_count as no_comments
              from videos'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["videotitle","no of comments"])
    st.write(df4)

elif question=="5. Videos with highest likes":
    query5='''SELECT 
                  video_name as videotitle,
                  channel_name as channel,
                  like_count as likecount
            from videos
            where like_count =
                    (select 
                        max(like_count) 
                    from videos)'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. Likes of all videos":
    query6='''select 
                  like_count as likecount,
                  video_name as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. Views of each channel":
    query7='''select Channel_name, Total_views from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. Videos published in the year of 2022":
    query8='''select 
                  video_name as videotitle,
                  publishedat ,
                  channel_name 
              from videos
              where EXTRACT(YEAR FROM publishedat) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","release_date","channelname"])
    st.write(df8)

elif question=="9. Average duration of all videos in each channel":
    query9='''select  
                  channel_name,
                  date_trunc('second', avg(duration)) as average_video_duration
           from videos
           group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","average_video_duration"])
    st.write(df9)

elif question=="10. Videos with highest number of comments":
    query10='''select 
                   video_name as videotitle,
                   channel_name,
	               comment_count as commentcount
            from videos
            where comment_count =
                    (select 
                        max(comment_count) 
                    from videos)'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)
