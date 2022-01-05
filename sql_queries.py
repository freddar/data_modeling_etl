# DROP TABLES

song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"
user_table_drop = "DROP TABLE IF EXISTS users;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"

# CREATE song_data dimension TABLES

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR NOT NULL PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT,
        duration FLOAT);
""")

artist_table_create = ("""
     CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR NOT NULL PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT);
""")

# CREATE log_data dimension & fact TABLES

# dimension TABLES

time_table_create = ("""
   CREATE TABLE IF NOT EXISTS time (
      start_time TIMESTAMP NOT NULL PRIMARY KEY,
      hour INT,
      day INT,
      week INT,
      month INT,
      year INT,
      weekday INT);    
""")

user_table_create = ("""
   CREATE TABLE IF NOT EXISTS users (
      user_id INT NOT NULL PRIMARY KEY,
      first_name VARCHAR,
      last_name VARCHAR,
      gender VARCHAR,
      level VARCHAR);
""")

# fact TABLE

songplay_table_create = ("""
   CREATE TABLE IF NOT EXISTS songplays (
       songplay_id SERIAL PRIMARY KEY,
       start_time TIMESTAMP NOT NULL,
       user_id INT NOT NULL,
       level VARCHAR,
       song_id VARCHAR,
       artist_id VARCHAR,
       session_id INT,
       location VARCHAR,
       user_agent VARCHAR);
""")

# INSERT RECORDS

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

time_table_insert = ("""
   INSERT INTO time (start_time, hour, day, week, month, year, weekday)
   VALUES (%s, %s, %s, %s, %s, %s, %s)
   ON CONFLICT DO NOTHING;
""")

user_table_insert = ("""
   INSERT INTO users (user_id, first_name, last_name, gender, level)
   VALUES (%s, %s, %s, %s, %s)
   ON CONFLICT(user_id) DO UPDATE SET level = excluded.level;
""")

songplay_table_insert = ("""
   INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
   VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
   SELECT so.song_id, ar.artist_id
   FROM songs so
   JOIN artists ar ON ar.artist_id = so.artist_id
   WHERE so.title = %s AND ar.name = %s AND so.duration = %s;
""")

# QUERY LISTS

create_table_queries = [song_table_create, artist_table_create, time_table_create, user_table_create, songplay_table_create]
drop_table_queries = [song_table_drop, artist_table_drop, time_table_drop, user_table_drop, songplay_table_drop]