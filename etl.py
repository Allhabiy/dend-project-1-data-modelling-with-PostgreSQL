import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Processing of all song records
    
    Purpose of this funciton is to loop in all the files from data/song_data folder
    using which the following tables are populated -
    song
    artist
    
    Parameters
    ----------
    cur : object
        Connection object passed on from the main function to perform the 
        data insertion into the tables
    filepath : string
        Filepath where all the song files are stored
    Returns
    ----------
    No return value
    
    Raises
    ----------
    DatabaseError
        If there are any database errors while inserting the data
    OSError
        If the filepaths are invalid
    
    """
    # open song file
    try:
        df = pd.read_json(filepath, lines=True)
    except OSError as err:
        print("OS error: {0}".format(err))

    # insert song record
    try:
        song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy().values.tolist()[0]
        cur.execute(song_table_insert, song_data)
    except psycopg2.DatabaseError as error:
        print("Error inserting data into songs table - {0}".format(error))
    
    # insert artist record
    try:
        artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].copy().values.tolist()[0]
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.DatabaseError as error:
        print("Error inserting data into artists table - {0}".format(error))    


def process_log_file(cur, filepath):
    """Processing of all log file records
    
    Purpose of this funciton is to loop in all the files from data/log_data folder
    using which the following tables are populated -
    time
    user
    songplay
    
    Parameters
    ----------
    cur : object
        Connection object passed on from the main function to perform the 
        data insertion into the tables
    filepath : string
        Filepath where all the song files are stored
    Returns
    ----------
    No return value
    
    Raises
    ----------
    DatabaseError
        If there are any database errors while inserting the data
    OSError
        If the filepaths are invalid
    
    """    
    # open log file
    try:
        df = pd.read_json(filepath, lines=True)
    except OSError as err:
        print("OS error: {0}".format(err))
    
    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"])
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
    
    # get all the column labels required for time table
    column_labels = ('timestamp','hour','day','week','month','year','weekday')
    
    # create dictionary data from the column_labels and time_data and then convert into data frame
    time_dictionary = dict(zip(column_labels, time_data))
    
    #load the data from dataframe
    time_df = pd.DataFrame.from_dict(time_dictionary)

    try:
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
    except psycopg2.DatabaseError as error:
        print("Error inserting data into time table - {0}".format(error))
        
    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    
    # insert user records
    try:
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
    except psycopg2.DatabaseError as error:
        print("Error inserting data into user table - {0}".format(error))

    # insert songplay records
    for index, row in df.iterrows():
        
        try:
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None
        except psycopg2.DatabaseError as error:
            print("Error fetching the song selection data - {0}".format(error))
            
        # insert songplay record
        songplay_data = ( pd.to_datetime(row.ts)
                        ,str(row.userId)
                        ,str(row.level)
                        ,songid
                        ,artistid
                        ,row.sessionId
                        ,str(row.location)
                        ,str(row.userAgent)
                        ) 
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.DatabaseError as error: 
            print("Error inserting rows into songplay table - {0}".format(error))            


def process_data(cur, conn, filepath, func):
    """Placeholder function to take a filepath and perform data extraction
    from it for a particular file extension i.e. json
    
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
    """Main object from which the sparkify database is populated by looping
    through songs and log files.
    """    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()