import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    This procedure processes a song file whose filepath has been provided as an argument.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    This procedure processes a log file whose filepath has been provided as an argument.
    It filters records by 'NextSong' action.
    It converts the timestamp column to datetime.
    It extracts the time information in order to store it into the time table.
    Then it extracts the user information in order to store it into the users table.
    Then it extracts the artist and song ids from the artists and songs tables and combines 
    this data with songplay information in order to store the combination into the songplays
    table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t.dt.strftime('%Y-%m-%d %I:%M:%S').values, t.dt.hour.values, t.dt.day.values, t.dt.weekofyear.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_time = pd.to_datetime(row.ts, unit='ms').strftime('%Y-%m-%d %I:%M:%S')
        songplay_data = [start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location,                                                                                row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    This procedure connects to the database, gets all log or song files matching extension from the filepath that has been provided as an argument.
    It prints the number of files found in the provided filepath.
    It iterates over all the files, processes each one of them with the function that has been provided 
    as an argument in order to extract, transform and load the right data into each one of the tables in the star schema that are fed by the function.
    Finally, after each file is processed and data stored in the right table of the database from that file,
    it prints the index of the processed file and the number of existing files. This shows how many files out of all the existing files where processed.

    INPUTS: 
    * cur the cursor variable to perform database operations
    * conn the connection variable that creates a new database session and connects to an existing database
    * filepath the directory path to the log or song files
    * func the process_song_file or the process_log_file functions
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
