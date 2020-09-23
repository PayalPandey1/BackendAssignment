from flask import Flask, jsonify, request
from googleapiclient.discovery import build
import sqlite3
import time
import json
import threading

app = Flask(__name__)

YOUTUBE_SEARCH_WORD = "cricket"
API_KEY_ARRAY = [
    "AIzaSyB7G1_mJMk_8q6DpPPPLdsZYq_WogmGHJI",
    "AIzaSyCJCIpiSPOwjcaXzZ4Ab2-lBjjsaIhFd4w",
    "AIzaSyB5V6g7I8P3zIHGwGmLl6rbz7bwRKPnrpM",
    "AIzaSyAsYaBtsFnvwnSdNBvTj8z2O93ypmdulAw",
]
UPDATE_FREQUENCY_IN_SECONDS = 10
API_PAGE_SIZE = 10
DATABASE_FILE_NAME = "videos.db"


def get_query_offset(page_param):
    # returns sql query offset from the
    # page query param
    page = 1
    try:
        parsed_page = int(page_param)
        if parsed_page > 0:
            page = parsed_page
    except (ValueError, TypeError):
        pass
    return (page - 1) * API_PAGE_SIZE


@app.route("/")
def home():
    return "<h2> Backend Assignment! </h2> <h4> </br> <i> /get_videos </i>  </br> <i> /search </i> </h4>"


@app.route("/get_videos")
def get_videos():
    try:
        conn = sqlite3.connect(DATABASE_FILE_NAME)
        curr = conn.cursor()
        result = []
        offset = get_query_offset(request.args.get("page"))
        for row in curr.execute(
            f"SELECT title, description, thumbnail_url, datetime(published_at), video_id FROM youtube ORDER BY datetime(published_at) DESC LIMIT {API_PAGE_SIZE} OFFSET {offset}"
        ):
            title, description, thumbnail_url, published_at, video_id = row
            dict = {}
            dict["title"] = title
            dict["description"] = description
            dict["thumbnail_url"] = thumbnail_url
            dict["published_at"] = published_at
            result.append(dict)

        return jsonify(code=200, result=result)
    except Exception as e:
        return jsonify(code=500, result="internal server error")


@app.route("/search_videos", methods=["GET"])
def search():
    query = request.args.get("query")
    print(query)
    offset = get_query_offset(request.args.get("page"))
    rows = search_the_database(query, offset)
    return jsonify(code=200, result=rows)


def search_the_database(query, offset):
    conn = sqlite3.connect(DATABASE_FILE_NAME)
    curr = conn.cursor()
    result = []
    curr.execute(
        f"SELECT title, description, thumbnail_url, datetime(published_at), video_id from youtube WHERE title LIKE '%{query}%' OR  description LIKE '%{query}%' ORDER BY datetime(published_at) DESC LIMIT {API_PAGE_SIZE} OFFSET {offset}"
    )
    conn.commit()
    rows_desc = curr.fetchall()
    conn.commit()
    result = []
    for row in rows_desc:
        title, description, thumbnail_url, published_at, video_id = row
        dict = {}
        dict["title"] = title
        dict["description"] = description
        dict["thumbnail_url"] = thumbnail_url
        dict["published_at"] = published_at
        result.append(dict)
    return result


def initialise_database():
    try:
        print("INFO: initialising the database")
        conn = sqlite3.connect(DATABASE_FILE_NAME)
        curr = conn.cursor()
        curr.execute(
            """CREATE TABLE IF NOT EXISTS youtube(
            title text,
            description text,
            thumbnail_url text,
            published_at DATETIME NOT_NULL,
            id INTEGER PRIMARY KEY,
            video_id INTEGER UNIQUE 
            )"""
        )
        conn.commit()
    except Exception as e:
        print("ERROR: Exception occurred while initialising the DB: {}".format(e))


def fetch_youtube_videos():
    try:
        global CURRENT_API_KEY_INDEX
        youtube = build(
            "youtube", "v3", developerKey=API_KEY_ARRAY[CURRENT_API_KEY_INDEX]
        )
        request = youtube.search().list(
            part="id,snippet",
            order="date",
            publishedAfter="2002-05-01T00:00:00Z",
            q="cricket",
            type="video",
            maxResults=100,
        )
        response = request.execute()
        list_items = []
        list_iter = response["items"]
        for items in list_iter:
            tuple_items = (
                items["snippet"]["title"],
                items["snippet"]["description"],
                items["snippet"]["thumbnails"]["default"]["url"],
                items["snippet"]["publishedAt"],
                items["id"]["videoId"],
            )
            list_items.append(tuple_items)
        return list_items
    except Exception as e:
        if check_youtube_quota_error(e):
            print("ERROR: error occurred while fetching videos, youtube quota error")
            change_the_api_key()
        else:
            print("ERROR: error occurred while fetching youtube videos: {}".format(e))


def check_youtube_quota_error(err_obj):
    errors = json.loads(err_obj.content)["error"]["errors"]
    is_quota_error = False
    for err in errors:
        if err["reason"] == "quotaExceeded":
            is_quota_error = True
            break
    return is_quota_error


def change_the_api_key():
    global CURRENT_API_KEY_INDEX
    print("INFO: changing API KEY")

    if CURRENT_API_KEY_INDEX == len(API_KEY_ARRAY) - 1:
        CURRENT_API_KEY_INDEX = 0
    else:
        CURRENT_API_KEY_INDEX += 1


def fetch_and_save_latest_results():
    print("INFO: fetching_and_saving the latest results in the database")
    try:
        list_items = fetch_youtube_videos()
        return list_items
    except Exception as e:
        print("ERROR: Exception occured while fetching videos: {}".format(e))


def update_database(list_items):
    try:
        print("INFO: updating the database")
        conn = sqlite3.connect(DATABASE_FILE_NAME)
        curr = conn.cursor()
        print("INFO: list_items: {}".format(len(list_items)))
        curr.executemany(
            "INSERT or IGNORE INTO youtube (title, description, thumbnail_url, published_at, video_id) VALUES (?, ?, ?, ?, ?)",
            list_items,
        )
        conn.commit()
        # curr.execute("select count(*) from youtube")
        # fixture_count = curr.fetchone()[0]
        # print("count_youtube: {}".format(fixture_count))

    except Exception as e:
        print("ERROR: Exception occurred while updating the DB: {}".format(e))


def setup_background_fetch():
    def background():
        while True:
            initialise_database()
            list_items = fetch_and_save_latest_results()
            time.sleep(UPDATE_FREQUENCY_IN_SECONDS)
            update_database(list_items)

    b = threading.Thread(name="background", target=background)
    b.start()


if __name__ == "__main__":
    """
    Main entry point into program execution

    PARAMETERS: none
    """
    global CURRENT_API_KEY_INDEX
    CURRENT_API_KEY = API_KEY_ARRAY[0]
    CURRENT_API_KEY_INDEX = 0
    initialise_database()
    print("INFO: Start the background fetch of the youtube videos")
    setup_background_fetch()  # start background fetch (asyncio)
    app.run(debug=True)
