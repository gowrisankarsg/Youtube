# importing the packages
import datetime
from numpy import datetime64
import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import urllib
import ssl
import sqlite3
import re
from googleapiclient.errors import HttpError

def convert_duration(duration):
                    match = re.search(r'PT(\d+)M(\d+)S',duration)
                    match1 = re.search(r'PT(\d+)H(\d+)M(\d+)S',duration)
                    match2 = re.search(r'PT(\d+)S',duration)

                    if match:
                        
                        minute = match.group(1)
                        sec = match.group(2)
                        if int(minute)<10 and int(sec)<10:
                            time = f'0{minute}:0{sec}'
                            return time
                        if int(minute)<10 and int(sec)>10:
                            time = f'0{minute}:{sec}'
                            return time
                        if int(minute)>10 and int(sec)<10:
                            time = f'{minute}:0{sec}'
                            return time    
                        if int(minute)>10 and int(sec)>10:
                            time = f'{minute}:{sec}'
                            return time
                    
                    if match1:
                        hour = match1.group(1)
                        minute = match1.group(2)
                        sec = match1.group(3)
                        
                        if int(minute)<10 and int(minute)<10 and int(sec)<10:
                            time = f'0{hour}:0{minute}:0{sec}'
                            return time
                        if int(minute)<10 and int(minute)<10 and int(sec)>10:
                            time = f'0{hour}:0{minute}:{sec}'
                            return time
                        if int(minute)<10 and int(minute)>10 and int(sec)<10:
                            time = f'0{hour}:{minute}:0{sec}'
                            return time
                        if int(minute)>10 and int(minute)<10 and int(sec)<10:
                            time = f'{hour}:0{minute}:0{sec}'
                            return time
                        if int(minute)>10 and int(minute)>10 and int(sec)>10:
                            time = f'{hour}:{minute}:{sec}'
                            return time
                    if match2:
                        sec = match2.group(1)
                        if int(sec)<10:
                            time = f'00:0{sec}'
                            return time
                        if int(sec)>10:
                            time = f'00:{sec}'
                            return time
                        
                    


def Get_data():
    # create mongodb connection
    username = "sankarallof" ## put your userid
    password = "Sankar2002" ### ur pass word

    # Encode the username and password
    encoded_username = urllib.parse.quote_plus(username)
    encoded_password = urllib.parse.quote_plus(password)

    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Construct the URI with encoded credentials
    uri = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.nb81bns.mongodb.net/?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'), tz_aware=False, connect=True)

    # create database name and collection name
    db = client['Youtube']
    collection = db['channels']
    
    # create project title name
    st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
    with st.sidebar:
        channel_name = st.text_input("Enter Channel Name")
        get_data = st.button("Get Data")
    if get_data:
    # create api connection
        def API_connect():
            API_key = "AIzaSyCD3UM3pwY1k-jE_hTmFqoGPnG_gwDeIDk"
            api_service_name = "youtube"
            api_version = "v3"
            youtube = build(api_service_name,api_version, developerKey=API_key)
            return youtube
        youtube = API_connect()

        # get channel id
        
        request = youtube.search().list(
            part = "id,snippet",
            channelType = "any",
            maxResults = 1,
            q = channel_name
        )
        response = request.execute()

        channel_id = response['items'][0]['id']['channelId']

        # get channel details
        request = youtube.channels().list(
            part = "snippet,contentDetails,statistics",
            id = channel_id)
        response = request.execute()

        channelDetails = dict(channelId = response['items'][0]['id'],
                            
                            channelName = response['items'][0]['snippet']['title'],
                            channelDescription = response['items'][0]['snippet']['description'],
                            subscriberCount = response['items'][0]['statistics']['subscriberCount'],
                            viewCount = response['items'][0]['statistics']['viewCount'],
                            videoCount = response['items'][0]['statistics']['videoCount'],
                            uploadId = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                            publishDate = response['items'][0]['snippet']['publishedAt'])
        uploadId = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        channelDetails_list = []
        channelDetails_list.append(channelDetails)

        # create channel dataframe
        #channel_df = pd.DataFrame(channelDetails_list)
        

        # get video id's
        playlist_id = uploadId

        def get_video_id(youtube,playlist_id):
            request = youtube.playlistItems().list(
                part = "contentDetails",
                playlistId = playlist_id,
                maxResults = 50
            )
            response = request.execute()

            Video_id = []
            for item in response['items']:
                video_id = item['contentDetails']['videoId']
                Video_id.append(video_id)

            next_page_token = response.get('nextPageToken')
            more_page = True

            while more_page:
                if next_page_token is None:
                    more_page = False
                else:
                    request = youtube.playlistItems().list(
                        part = "snippet,contentDetails",
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
                    response = request.execute()

                    for item in response['items']:
                        video_id = item['contentDetails']['videoId']
                        Video_id.append(video_id)

                    next_page_token = response.get('nextPageToken')

            return Video_id
        
        ids1 = get_video_id(youtube,playlist_id)
        
        # get video details
        def get_video_details(youtube,ids1):
            video_detais = []
            for i in ids1:
                request = youtube.videos().list(
                    part = "snippet,contentDetails,statistics",
                    id = i)

                response = request.execute()

                
                
                for video in response['items']:
                    time = convert_duration(video['contentDetails']['duration'])
                    videos = dict(
                        ChannelId = video['snippet']['channelId'],
                        Video_Id = i,
                        Video_title = video['snippet']['title'],
                        Video_Duration = time,
                        Video_Description = video['snippet']['description'],
                        Video_PublishDate = video['snippet']['publishedAt'],
                        Video_ViewCount = video['statistics']['viewCount'],
                        Video_LikeCount = video['statistics']['likeCount'],
                        Video_CommentCount = video['statistics']['commentCount']
                    )
                    video_detais.append(videos)

            return video_detais
        
        cv = get_video_details(youtube,ids1)

        

        # get comment details
        def get_comment_details(id):
            
            comments = []
            next_page_token = None

            while True:
                for i in id:
                    request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=i,
                        textFormat="plainText",
                        pageToken=next_page_token,
                        maxResults = 3
                    )
                    try:
                        response = request.execute()

                        for item in response["items"]:
                            comment_detail = dict(commentId = item["snippet"]["topLevelComment"]["id"],
                            videoId = item["snippet"]["videoId"],
                            commentAuthorName = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            commentText = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            commentPublishDate = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                            commentLikeCount = item["snippet"]["topLevelComment"]["snippet"]["likeCount"],
                            commentReplyCount = item["snippet"]["totalReplyCount"])
                            comments.append(comment_detail)
                    except HttpError as e:
                        pass

                next_page_token = response.get("nextPageToken")

                if not next_page_token:
                    break

                return comments
            
        
        com = get_comment_details(ids1)

        video_dict = {}
        for i, video in enumerate(cv):
            video_dict[f'video_{i}'] = video

        comment_dict = {}
        if com is not None:
            for i, comment in enumerate(com):
                comment_dict[f'comment_{i}'] = comment
        else:
            with st.sidebar:
                st.error("Comment is disabled")

        channel_Information = {
            'ChannelDetails': channelDetails,
            'VideoDetails': video_dict,
            'CommentDetails': comment_dict
        }
                

        # create json for save mongodb
        #channel_Information = dict(ChannelDetails=channelDetails,VideoDetails=cv,CommentDetails=com)

        #channel_Information = dict(ChannelDetails=channelDetails,VideoDetails=video_detais,CommentDetails=comments)

        if collection.find_one({'ChannelDetails.channelName' : channel_Information['ChannelDetails']['channelName']}):
            with st.sidebar:
                st.warning("This channel name is already existed")
                st.warning("Please try another channel name")
        else:
            
            st.json(channel_Information)
            collection.insert_one(channel_Information)
            with st.sidebar:
                st.success("This channel information is successfully inserted")


def clean_process():

    

    # create mongodb connection
    username = "sankarallof" ## put your userid
    password = "Sankar2002" ### ur pass word

    # Encode the username and password
    encoded_username = urllib.parse.quote_plus(username)
    encoded_password = urllib.parse.quote_plus(password)

    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Construct the URI with encoded credentials
    uri = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.nb81bns.mongodb.net/?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'), tz_aware=False, connect=True)

    # create database name and collection name
    db = client['Youtube']
    collection = db['channels']

    # create dropdown for channel name list
    channelList = []
    for i in collection.find({},{'ChannelDetails.channelName':1, '_id':0}):
        channelList.append(i['ChannelDetails']['channelName'])
    # create project title name
    st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
    with st.sidebar:
        option = st.selectbox("Select channel name",channelList,index=None,placeholder='Selct channel name')
        migrate = st.button("Migrate")
    channel = []
    for i in collection.find({'ChannelDetails.channelName':option},{'ChannelDetails':1, '_id':0}):
        channel.append(i['ChannelDetails'])
    video = []
    
    for i in collection.find({'ChannelDetails.channelName':option},{'VideoDetails':1, '_id':0}):
        data = i['VideoDetails']
        for j in range(0,len(data)):
            video.append(data[f'video_{j}'])
        
    comment = []
    for i in collection.find({'ChannelDetails.channelName':option},{'CommentDetails':1, '_id':0}):
        data = i['CommentDetails']
        for j in range(0,len(data)):
            comment.append(data[f'comment_{j}'])
    channel_df = pd.DataFrame(channel)

    video_df = pd.DataFrame(video)
    commment_df = pd.DataFrame(comment)
    
    if option:
        
        st.write("Channel Detail Dataframe")
        st.dataframe(channel_df)
        st.write("Video Detail Dataframe")
        st.dataframe(video_df)
        st.write("Comment Detail Dataframe")
        st.dataframe(commment_df)

    # Insert data to sql

    if migrate:
        # create sql connection
        conn = sqlite3.connect("youtube.db")
        cursor = conn.cursor()

        # create tables
        channel_table = ''' CREATE TABLE IF NOT EXISTS channel (
            
            channelId TEXT ,
            channelName TEXT PRIMARY KEY,
            channelDescription TEXT,
            subscriberCount INTEGER,
            viewCount INTEGER,
            videoCount INTEGER,
            uploadId TEXT,
            publishDate DATE
        )'''

        video_table = ''' CREATE TABLE IF NOT EXISTS video (
            
            ChannelId TEXT,
            Video_Id TEXT PRIMARY KEY,
            Video_title TEXT,
            Video_Duration TEXT,
            Video_Description TEXT,            
            Video_PublishDate DATE,
            Video_ViewCount INTEGER,
            Video_LikeCount INTEGER,
            Video_CommentCount INTEGER

        )'''

        comment_table = ''' CREATE TABLE IF NOT EXISTS comment (
            
            commentID TEXT ,
            videoId TEXT,
            commentAuthorName TEXT,
            commentText TEXT,
            commentPulishDate DATE,
            commentLikeCount INTEGER,
            commentReplyCount INTEGER
        )'''

        cursor.execute(channel_table)
        cursor.execute(video_table)
        cursor.execute(comment_table)


        channel_df['subscriberCount'] = channel_df['subscriberCount'].astype(int)
        channel_df['viewCount'] = channel_df['viewCount'].astype(int)
        channel_df['videoCount'] = channel_df['videoCount'].astype(int)
        channel_df['publishDate'] = pd.to_datetime(channel_df['publishDate']).dt.date
        #channel_df['PublishDate'] = channel_df['publishDate'].dt.strftime("%Y-%m-%d")
        #channel_df['PublishDate'] = pd.to_datetime(channel_df['PublishDate'] , format="%Y-%m-%d")
        
        
        
        
        video_df['Video_ViewCount'] = video_df['Video_ViewCount'].astype(int)
        video_df['Video_LikeCount'] = video_df['Video_LikeCount'].astype(int)
        video_df['Video_CommentCount'] = video_df['Video_CommentCount'].astype(int)
        video_df['Video_PublishDate'] = pd.to_datetime(video_df['Video_PublishDate']).dt.date
        #video_df['Video_publishDate'] = video_df['Video_PublishDate'].dt.strftime("%Y-%m-%d")
        #video_df['Video_publishDate'] = pd.to_datetime(video_df['Video_publishDate'],format="%Y-%m-%d")

        commment_df['commentPublishDate'] = pd.to_datetime(commment_df['commentPublishDate']).dt.date
        #commment_df['commentPublish_Date'] = commment_df['commentPublishDate'].dt.strftime("%Y-%m-%d")
        #commment_df['commentPublish_Date'] = pd.to_datetime(commment_df['commentPublish_Date'],format="%Y-%m-%d")

        channel_sql = "SELECT * FROM channel"
        channel_sql_df = pd.read_sql_query(channel_sql,conn)
        st.dataframe(channel_sql_df)
        
        checkName = list(channel_sql_df['channelName'])
        
        if option in checkName:
            with st.sidebar:
                st.warning("This channel name already existed")
        else:
            for row in channel_df.itertuples():
                cursor.execute("INSERT INTO channel (channelId, channelName, channelDescription, subscriberCount, viewCount, videoCount, uploadId, publishDate) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))

            for row in video_df.itertuples():
                cursor.execute(f"INSERT INTO video (channelId,Video_Id,Video_title,Video_Duration,Video_Description,Video_PublishDate,Video_ViewCount,Video_LikeCount,Video_CommentCount) VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)",(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],row[9]))

            for row in commment_df.itertuples():
                cursor.execute(f"INSERT INTO comment (commentID,videoId,commentAuthorName,commentText,commentPulishDate,commentLikeCount,commentReplyCount) VALUES (?,?,?,?,?,?,?)",(row[1], row[2], row[3], row[4], row[5], row[6], row[7]))

            
            with st.sidebar:
                st.success("Data Successfully Inserted")
                
            conn.commit()
            conn.close()


def Queries():
    conn = sqlite3.connect("youtube.db")
    cursor = conn.cursor()

    query = "SELECT * FROM channel INNER JOIN video ON channel.channelId = video.channelId"
    query2 = "SELECT * FROM video INNER JOIN comment ON video.Video_Id = comment.videoId"

    channel_video = pd.read_sql_query(query,conn)
    video_comment = pd.read_sql_query(query2,conn)

    queries = ["1.What are the names of all the videos and their corresponding channels?","2.Which channels have the most number of videos, and how many videos do they have?","3.What are the top 10 most viewed videos and their respective channels?","4.How many comments were made on each video, and what are their corresponding video names?","5.Which videos have the highest number of likes, and what are their corresponding channel names?","6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?","7.What is the total number of views for each channel, and what are their corresponding channel names?","8.What are the names of all the channels that have published videos in the year 2022?","9.What is the average duration of all videos in each channel, and what are their corresponding channel names?","10.Which videos have the highest number of comments, and what are their corresponding channel names?"]
    with st.sidebar:
        option = st.selectbox("Select the Query",queries,index=None,placeholder="Select the Query")

    if option == queries[0]:
        st.title(queries[0])
        st.subheader("channelname ana videoname Dataframe")
        st.dataframe(channel_video[['channelName','Video_title']])

    if option == queries[1]:
        st.title(queries[1])

        MN_channel = channel_video.groupby(['channelName'])['Video_title'].count().idxmax()
        st.write(f"Most number of videos having channel name is : {MN_channel}")
        MN_channel_df = channel_video.groupby(['channelName'])['Video_title'].count().reset_index().rename(columns={"Video_title":"Video_Count"})
        st.subheader("Dataframee")
        st.dataframe(MN_channel_df)



    if option == queries[2]:
        st.title(queries[2])
        top10views = channel_video.sort_values(by=["Video_ViewCount"],ascending=False).head(10)[['channelName','Video_title','Video_ViewCount']]
        st.subheader("Top 10 video view count Dataframe")
        st.dataframe(top10views)

    if option == queries[3]:
        st.title(queries[3])
        st.subheader("Comment count Dataframe")
        comment_count = video_comment.groupby(['Video_title'])['commentText'].count().reset_index().rename(columns={"commentText":"commentCount"})
        st.dataframe(comment_count)

    if option == queries[4]:
        st.title(queries[4])
        st.subheader("High No likes of video Dataframe")
        likecount = channel_video.iloc[channel_video.groupby(['Video_title'])['Video_LikeCount'].idxmax()][["channelName","Video_title","Video_LikeCount"]]
        st.dataframe(likecount.iloc[likecount['Video_LikeCount'].idxmax()].reset_index())

    if option == queries[5]:
        st.title(queries[5])
        st.subheader("Total number of likes in each video Dataframe")
        st.dataframe(channel_video[['Video_title',"Video_LikeCount"]])

    if option == queries[6]:
        st.title(queries[6])
        st.subheader("Total number of views in each channel")
        channel = pd.read_sql_query("SELECT * FROM channel",conn)
        st.dataframe(channel[['channelName','viewCount']])

    if option == queries[7]:
        st.title(queries[7])
        st.subheader("publish videos in year 2022 Dataframe")
        channel_video['Video_PublishYaer'] = pd.to_datetime(channel_video['Video_PublishDate']).dt.year
        st.dataframe(channel_video[channel_video['Video_PublishYaer']==2022][["channelName","Video_title","Video_PublishDate"]])

    if option == queries[8]:
        st.title(queries[8])
        channel_video['Video_Duration (seconds)'] = (pd.to_datetime(channel_video['Video_Duration'],format="%M:%S").dt.minute*60)+(pd.to_datetime(channel_video['Video_Duration'],format="%M:%S").dt.second*60)
        dur_mean=channel_video['Video_Duration (seconds)'].mean()

        st.subheader(f"Video average duration is {int(dur_mean//60):02d}M:{int(dur_mean%60):02d}S")
        

    if option == queries[9]:
        st.title(queries[9])
        st.subheader("High comment count having video Dataframe")
        st.dataframe(channel_video.iloc[channel_video.groupby(['channelName'])['Video_CommentCount'].idxmax()][['channelName','Video_title','Video_CommentCount']])

    conn.commit()
    conn.close()

with st.sidebar:
    func = st.selectbox("Select the Function name",['Get_data','clean_process','Queries'],index=None,placeholder="Select the Function name")

if "Get_data" == func:
    Get_data()

if "clean_process" == func:
    clean_process()

if "Queries" == func:
    Queries()




        

        


  




    
