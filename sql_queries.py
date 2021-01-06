
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (
    """create table if not exists staging_events (
    artist varchar, 
    auth varchar, 
    firstName varchar, 
    gender varchar, 
    itemInSession integer, 
    lastName varchar, 
    length double precision, 
    level varchar, 
    location varchar,
    method varchar,
    page varchar,
    registration double precision,
    sessionId integer,
    song varchar,
    status integer,
    ts bigint,
    userAgent varchar,
    userId varchar)"""
)

staging_songs_table_create = (
    """create table if not exists staging_songs (
    num_songs integer,
    artist_id varchar, 
    artist_latitude double precision, 
    artist_longitude double precision, 
    artist_location varchar, 
    artist_name varchar, 
    song_id varchar, 
    title varchar, 
    duration double precision, 
    year int)"""
)

#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = (
    """create table if not exists songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp not null, 
    user_id varchar not null, 
    level varchar, 
    song_id varchar not null, 
    artist_id varchar not null, 
    session_id int, 
    location varchar, 
    user_agent varchar)"""
)

# user_id, first_name, last_name, gender,level 
user_table_create = (
    """create table if not exists users(
    user_id varchar PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar)"""
)

# song_id, title, artist_id, year, duration
song_table_create = (
    """create table if not exists songs(
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar not null, 
    year int, 
    duration numeric)"""
)

# artist_id, name, location, latitude, longitude
artist_table_create = (
    """create table if not exists artists(
    artist_id varchar PRIMARY KEY, 
    name varchar, 
    location varchar, 
    latitude numeric, 
    longitude numeric)"""
)

# start_time, hour, day, week, month, year, weekday
time_table_create = (
    """create table if not exists time(
    start_time TIMESTAMP PRIMARY KEY, 
    hour int not null, 
    day int not null, 
    week int not null, 
    month int not null, 
    year int not null, 
    weekday varchar not null)"""
)

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
        FROM {}
        iam_role {}
        FORMAT AS JSON {}
        """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs
        FROM {}
        iam_role {}
        FORMAT JSON AS 'auto'
        """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES
user_table_insert = (
    """INSERT INTO users 
    (user_id, first_name, last_name, gender, level) 
    SELECT DISTINCT userId as user_id, firstName, lastName, gender, level
    FROM staging_events"""
)

artist_table_insert = (
    """INSERT INTO artists 
    (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs"""
)

song_table_insert = (
    """INSERT INTO songs 
    (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT song_id, title, artist_id, year, duration 
    FROM staging_songs"""
)
print("Inseretd data into songs")

songplay_table_insert = (
    """INSERT INTO songplays (
    start_time, user_id,level,song_id, artist_id, session_id, location, user_agent) 
    select timestamp 'epoch' + CAST(staging_events.ts AS BIGINT)/1000 * INTERVAL '1 second' as start_time, 
        userId as user_id, 
        level, 
        songs.song_id as song_id, 
        artists.artist_id as artist_id,
        sessionId as session_id, 
        staging_events.location as location, 
        userAgent as user_agent
        from staging_events 
        inner join artists on artists.name = staging_events.artist
        inner join songs on songs.title = staging_events.song 
        where page='NextSong'"""
)
print("songplays table insertion done")

time_table_insert = (
    """INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    SELECT DISTINCT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' as start_time,
    extract(hour from timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * INTERVAL '1 second') as hour,
    extract(day from timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * INTERVAL '1 second') as day,
    extract(week from timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * INTERVAL '1 second') as week,
    extract(month from timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * INTERVAL '1 second') as month,
    extract(year from timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * INTERVAL '1 second') as year,
    extract(weekday from timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * INTERVAL '1 second') as weekday
    from staging_events se"""
)


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


