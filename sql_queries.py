import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE','ARN')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events cascade"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs cascade"
songplay_table_drop = "DROP TABLE IF EXISTS songplay cascade"
user_table_drop = "DROP TABLE IF EXISTS users cascade"
song_table_drop = "DROP TABLE IF EXISTS songs cascade"
artist_table_drop = "DROP TABLE IF EXISTS artists cascade"
time_table_drop = "DROP TABLE IF EXISTS time cascade"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    event_id          INTEGER IDENTITY(1,1),
    artist            VARCHAR,
    auth              VARCHAR,
    firstName         VARCHAR,
    gender            CHAR(1),
    itemInSession     INTEGER,
    lastName          VARCHAR,
    length            Numeric,
    level             VARCHAR,
    location          VARCHAR,
    method            VARCHAR,
    page              VARCHAR,
    registration      BIGINT,
    sessionId         INTEGER,
    song              VARCHAR,
    status            INTEGER,
    ts                TIMESTAMP,
    userAgent         VARCHAR,
    userId            INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs          INTEGER,
    artist_id          VARCHAR,
    artist_latitude    VARCHAR,
    artist_longitude   VARCHAR,
    artist_location    VARCHAR,
    artist_name        VARCHAR,
    song_id            VARCHAR,
    title              VARCHAR,
    duration           Numeric,
    year               INTEGER
)
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id            INTEGER PRIMARY KEY NOT NULL SORTKEY DISTKEY, 
    first_name         VARCHAR, 
    last_name          VARCHAR, 
    gender             CHAR(1), 
    level              VARCHAR
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id            VARCHAR PRIMARY KEY NOT NULL SORTKEY, 
    title              VARCHAR NOT NULL, 
    artist_id          VARCHAR, 
    year               INTEGER, 
    duration           NUMERIC NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id         VARCHAR PRIMARY KEY NOT NULL SORTKEY, 
    name              VARCHAR not null, 
    location          VARCHAR, 
    latitude          DOUBLE PRECISION, 
    longitude         DOUBLE PRECISION
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time       TIMESTAMP PRIMARY KEY NOT NULL SORTKEY, 
    hour             INTEGER, 
    day              INTEGER, 
    week             INTEGER, 
    month            INTEGER, 
    year             INTEGER, 
    weekday          INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id     INTEGER IDENTITY(1,1) PRIMARY KEY NOT NULL SORTKEY,
    start_time      TIMESTAMP NOT NULL REFERENCES time(start_time), 
    user_id         INTEGER NOT NULL REFERENCES users(user_id) DISTKEY, 
    level           VARCHAR, 
    song_id         VARCHAR REFERENCES songs(song_id), 
    artist_id       VARCHAR REFERENCES artists(artist_id), 
    session_id      INTEGER, 
    location        VARCHAR(300), 
    user_agent      VARCHAR(300)
    )
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON {}
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, 
artist_id, session_id, location, user_agent)
select se.ts,
       se.userId      as user_id,
       se.level       as level,
       ss.song_id     as song_id,
       ss.artist_id   as artist_id,
       se.sessionId   as session_id,
       se.location    as location,
       se.userAgent   as user_agent
from staging_events se 
join staging_songs ss
on se.artist = ss.artist_name and page = 'NextSong'
""")

user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, level)
select distinct userId               as user_id
                firstName            as first_name,
                lastName             as last_name,
                gender               as gender,
                level                as level
from staging_events
where userId IS NOT NULL and page = 'NextSong'
""")

song_table_insert = ("""
insert into songs(song_id, title, artist_id, year, duration)
select distinct song_id,
                title,
                artist_id,
                year,
                duration
from staging_songs
where song_id IS NOT NULL
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude)
select distinct artist_id        as artist_id,
                artist_name      as name,
                artist_location  as location,
                artist_latitude  as latitude,
                artist_longitude as longitude
from staging_songs
where artist_id IS NOT NULL
""")

time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year, weekday)
select ts                         as start_time,
       EXTRACT(HOUR from ts)      as hour,
       EXTRACT(DAY  from ts)      as day,
       EXTRACT(WEEK from ts)      as week,
       EXTRACT(MONTH from ts)     as month,
       EXTRACT(YEAR from ts)      as year,
       EXTRACT(DAYOFWEEK from ts) as weekday
from staging_events
where ts IS NOT NULL and page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
