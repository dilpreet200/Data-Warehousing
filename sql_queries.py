import configparser


# CONFIG
config = configparser.ConfigParser() # "config" is an object of configparser class and this class from configparser modules helps to read and write config files which in this case is "dwh.cfg"
config.read('dwh.cfg') # using object to read config file

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist; "
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
        artist VARCHAR, auth VARCHAR,firstName VARCHAR, gender  VARCHAR, itemInSession INTEGER,lastName  VARCHAR, length FLOAT,
        level VARCHAR, location  VARCHAR,  method  VARCHAR, page VARCHAR, registration  FLOAT, sessionId INTEGER,
        song VARCHAR,status INTEGER,ts  TIMESTAMP,userAgent  VARCHAR,userId  INTEGER 
    )
""")

staging_songs_table_create = (""" CREATE TABLE staging_songs(
        num_songs  INTEGER,artist_id VARCHAR,artist_latitude FLOAT,artist_longitude FLOAT,
        artist_location VARCHAR, artist_name  VARCHAR, song_id VARCHAR, title  VARCHAR,
        duration FLOAT, year  INTEGER
    )
""")

songplay_table_create = ("""CREATE TABLE songplays(
        songplay_id         INTEGER       PRIMARY KEY,
        start_time          TIMESTAMP       NOT NULL ,
        user_id             INTEGER       NOT NULL,
        level               VARCHAR,
        song_id             VARCHAR    NOT NULL,
        artist_id           VARCHAR         NOT NULL,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
    )
""")

user_table_create = ("""    CREATE TABLE users(
        user_id             INTEGER         NOT NULL PRIMARY KEY,
        first_name          VARCHAR         NOT NULL,
        last_name           VARCHAR         NOT NULL,
        gender              VARCHAR         NOT NULL,
        level               VARCHAR         NOT NULL
    )
""")

song_table_create = ("""CREATE TABLE songs(
        song_id             VARCHAR         NOT NULL PRIMARY KEY,
        title               VARCHAR         NOT NULL,
        artist_id           VARCHAR         NOT NULL,
        year                INTEGER         NOT NULL,
        duration            FLOAT
    )
""")

artist_table_create = ("""CREATE TABLE artists(
        artist_id           VARCHAR         NOT NULL PRIMARY KEY,
        name                VARCHAR         NOT NULL,
        location            VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT
    )
""")

time_table_create = (""" CREATE TABLE time(
        start_time          TIMESTAMP       NOT NULL  PRIMARY KEY,
        hour            INTEGER        NOT NULL,
        day                 INTEGER         NOT NULL,
        week            INTEGER       NOT NULL,
        month              INTEGER         NOT NULL,
        year                INTEGER         NOT NULL,
        weekday             VARCHAR(20)     NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  ==  'NextSong'
""")


user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page  ==  'NextSong';
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    ;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name             AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    ;
""")
time_table_insert =("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)             AS hour,
            EXTRACT(day FROM start_time)               AS day,
            EXTRACT(week FROM start_time)             AS week,
            EXTRACT(month FROM start_time)           AS month,
            EXTRACT(year FROM start_time)            AS year,
            EXTRACT(dayofweek FROM start_time)     as weekday
    FROM songplays;
""") 

# QUERY LISTS

""""create_tables.py""""

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

""""etl.py""""

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
