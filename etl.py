import os
import glob
import psycopg2
import pandas as pd
from sql_queries import insert_table_queries, song_select


def process_song_file(cur, filepath):

    """
    This method starts by reading the file that is passed with the file
    curser of the database connection. 
    Then it execute the insert queries of the song and artist tables 

    ** I took the sql statements from the sql_queries.py file **
    """
    df = pd.read_json(filepath, lines=True)

    song_data = df[["song_id", "title", "artist_id", \
        "year", "duration"]].values[0].tolist()
    cur.execute(insert_table_queries[0], song_data)
    
    artist_data = df[["artist_id", "artist_name", "artist_location",\
     "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(insert_table_queries[1], artist_data)


def process_log_file(cur, filepath):

    """
    This method starts by reading the file that is passed with the file
    curser of the database connection. 

    Then it execute the insert queries of the time and user tables 

    ** I took the sql statements from the sql_queries.py file **
    """
    df = pd.read_json(filepath, lines=True)

    df =  df[(df['page'] == 'NextSong')]

    t = pd.to_datetime(df['ts'], unit='ms')
    df.ts = t

    time_data = list((t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month,\
         t.dt.year, t.dt.weekday))
    column_labels = list(('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'))
    time_df =  pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(insert_table_queries[2], list(row))

    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    for i, row in user_df.iterrows():
        cur.execute(insert_table_queries[3], row)

    """
    Then it execute the select query of the song and artist tables 
    and insert into the songplays table

    ** I took the sql statements from the sql_queries.py file **
    """
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
        songplay_data = (index, row.ts, row.userId, row.level, songid, \
            artistid, row.sessionId,row.location, row.userAgent)
        cur.execute(insert_table_queries[4], songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()