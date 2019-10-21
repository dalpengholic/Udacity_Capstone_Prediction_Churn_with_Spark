from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, FloatType
from pyspark.sql import Window
import re
from pyspark.sql.functions import min, max, size, collect_set, count, when, col, desc, asc, round, sum
from datetime import datetime

def ETL_for_ML(df):
    '''
    Function to extract, transform, and load data for ML
    Input:
    1. df: pyspark dataframe from event_filepath
    Outout:
    2. df_new1: pyspark dataframe after ETL
    '''

    def subfunc_cleaning(df):
        '''
        Remove rows that userId column have only whitespace
        '''
        # Remove no id rows
        df = df.filter(df["userId"] != "")
        return df

    def subfunc_featureExtraction(df):
        '''
        Function do basic feature extraction.
        The order of the extraction of the features is
        1. 'churn'
        2. 'os_system' (from userAgent)
        3. 'location_frist' (from location)
        4. 'total_sessionId' 
        5. 'total_itemInSession' 
        6. 'last_access_time'
        7. 'first_access_time'
        8. 'last_level'
        9. 'time_after_id_creation_hour' 
        10. 'num_week'
        11. 'num_month'
        12. 'num_year'
        13. 'top100_artist_alltime'
        14. 'total_Top100_artist_alltime'
        15. 'top100_song_week'
        16. 'total_Top100_song_week'
        Input:
        1. df: pyspark dataframe from subfunc_cleaning(df)
        Output:
        1. df : pyspark dataframe after feature extraction
        '''
        # Make a list of ids who churned
        churn_id_list = df[df.page == "Cancellation Confirmation"].select("userId").distinct().collect()
        churn_id_list = [x['userId'] for x in churn_id_list]
        create_churn_udf = udf(lambda userid: 1 if userid in churn_id_list else 0, IntegerType())
        df = df.withColumn("churn", create_churn_udf(df.userId))

        # Create new userAgent column
        def create_new_agent(userAgent):
            if userAgent == None:
                computer_os = None
            else:
                computer_os = userAgent.split()[1]
                computer_os = re.sub(r'[\W\s]','' ,computer_os)
            return  computer_os
        create_new_agent_udf = udf(create_new_agent, StringType())
        df = df.withColumn("os_system", create_new_agent_udf(df.userAgent))


        # Create new location column
        def create_new_location(location):
            if location == None:
                location_first = None
            else:
                location_first = location.split(',')[-1].split('-')[0].strip()
            return location_first
        create_new_location_udf = udf(create_new_location, StringType())
        df = df.withColumn("location_first", create_new_location_udf(df.location))


        # Create total number of sessionId column
        w = Window.partitionBy(df.userId)
        df = df.withColumn('total_sessionId', size(collect_set('sessionId').over(w)))


        # Create total number of itemInSession column
        df = df.withColumn('total_itemInSession', count('itemInSession').over(w))


        # Create last_access_time,first_access_time columns
        df = df.withColumn('last_access_time', max('ts').over(w))
        df = df.withColumn('first_access_time', min('ts').over(w))


        # Create last_level column
        df = df.withColumn('last_level',when(df.last_access_time == df.ts, df.level))


        # Create time difference column
        def calculate_time_after_id_creation(last_time, first_time):
            last_access_datetime = datetime.utcfromtimestamp(last_time / 1000)
            first_access_datetime = datetime.utcfromtimestamp(first_time / 1000)
            time_after_id_creation = last_access_datetime - first_access_datetime
            result = time_after_id_creation.total_seconds()/3600

            return result

        calculate_time_after_id_creation_udf = udf(calculate_time_after_id_creation, FloatType())
        df = df.withColumn("time_after_id_creation_hour", calculate_time_after_id_creation_udf(df.last_access_time, df.first_access_time))
        df = df.withColumn("time_after_id_creation_hour", round(col('time_after_id_creation_hour')/1, 2))


        # convert timestamp to date (string)
        def create_numWeek(ts):
            return datetime.utcfromtimestamp(ts / 1000).strftime("%V")
        def create_numMonth(ts):
            return datetime.utcfromtimestamp(ts / 1000).strftime("%m")
        def create_numYear(ts):
            return datetime.utcfromtimestamp(ts / 1000).strftime("%Y")
        create_numWeek_udf = udf(create_numWeek, StringType())
        create_numMonth_udf = udf(create_numMonth, StringType())
        create_numYear_udf = udf(create_numYear, StringType())
        df = df.withColumn('num_week', create_numWeek_udf(col('ts')))
        df = df.withColumn('num_month', create_numMonth_udf(col('ts')))
        df = df.withColumn('num_year', create_numYear_udf(col('ts')))


        # Make a top_100 alltime artist list
        tmp_list = df.where(df.artist != "").groupby("artist").count().sort(col("count").desc()).collect()[0:100]
        top_100_alltime_artist_list = [row["artist"] for row in tmp_list]
        top_100_alltime_artist_list


        # Make udf to set 1 at churn column in every row of the chrun users 
        def create_top100_alltime(artist):
            if artist in top_100_alltime_artist_list:
                return 1
            else:
                return 0
        create_top100_alltime_udf = udf(create_top100_alltime, IntegerType())
        df = df.withColumn("top100_artist_alltime", create_top100_alltime_udf(df.artist))
        # Count total number of top 100
        w = Window.partitionBy(df.userId)
        df = df.withColumn('total_Top100_artist_alltime', sum('top100_artist_alltime').over(w))


        # Make a dictionary of a best seller song list of each week
        tmp_list = df.select("num_week").distinct().sort("num_week").collect()
        available_week_list = [row["num_week"] for row in tmp_list]
        available_week_list


         # Make a dictionary of a best seller song list of each week
        def create_dict_top100_song_week(available_week_list):
            dict_top100_song_week = dict()
            for week in available_week_list:
                top_100_week_song_list = df.where((df.artist != "") & (df.num_week == week)).groupby("song","num_week").count()\
                .sort(col("num_week"), col("count").desc()).collect()[0:100]
                top_100_week_song_list = [row['song'] for row in top_100_week_song_list]
                dict_top100_song_week[week] = top_100_week_song_list
            return dict_top100_song_week
        dict_top100_song_week = create_dict_top100_song_week(available_week_list)

        # Make a top_100_song_week list
        def create_top100_song_week(song, num_week):
            if song in dict_top100_song_week[num_week]:
                return 1
            else:
                return 0

        create_top100_song_week_udf = udf(create_top100_song_week, IntegerType())
        df = df.withColumn("top100_song_week", create_top100_song_week_udf(df.song, df.num_week))
        # Count total number of top 100_song_week
        w = Window.partitionBy(df.userId)
        df = df.withColumn('total_Top100_song_week', sum('top100_song_week').over(w))
        
        return df
    
    def subfunc_joinDf_featureExtraction(df):
        '''
        Function to join other dataframes and extract additional features
        Other dataframes made from df by where, groupBy, agg, count methods are
        'thumbsup_df', 'thumbsdown_df',
        'advert_df, 'addfriend_df',
        'addplaylist_df', 'sub_upgrade_df'
        'sub_downgrade_df', 'error_df'
        'logout_df', 'last_level_df',
        'spent_time_df'
        Input:
        1. df: Pyspark dataframe from subfunc_featureExtraction(df)
        Output:
        2. df_new1: Pyspark dataframe after joining and feature extraction
        '''
        # Create other dataframes to be joined to df dataframe to get 
        # Number of thumb up/ thumb down/ advert/ add friend/ upgrade/ downgrade/ error/ logout
        thumbsup_df = df.where(df.page == 'Thumbs Up').groupBy("userId").agg(count("page").alias("total_thumbsup"))
        thumbsdown_df = df.where(df.page == 'Thumbs Down').groupBy("userId").agg(count("page").alias("total_thumbsdown"))
        advert_df =  df.where(df.page == 'Roll Advert').groupBy("userId").agg(count("page").alias("total_rolladvert"))
        addfriend_df =  df.where(df.page == 'Add Friend').groupBy("userId").agg(count("page").alias("total_addfriend"))
        addplaylist_df =  df.where(df.page == 'Add to Playlist').groupBy("userId").agg(count("page").alias("total_addplaylist"))
        sub_upgrade_df = df.where(df.page == 'Submit Upgrade').groupBy("userId").agg(count("page").alias("total_sub_upgrade"))
        sub_downgrade_df = df.where(df.page == 'Submit Downgrade').groupBy("userId").agg(count("page").alias("total_sub_downgrade"))
        error_df = df.where(df.page == 'Error').groupBy("userId").agg(count("page").alias("total_error"))
        logout_df = df.where(df.page == 'Logout').groupBy("userId").agg(count("page").alias("total_logout"))
        last_level_df = df[df.last_level != 'None'].select("userId","last_level")
        
        # Create total spent time
        spent_time_df = df.groupBy("userId", "sessionId").agg(max('ts').alias("max_ts_session"), \
        min('ts').alias("min_ts_session")).orderBy("sessionId", ascending=True)

        def calculate_time_inSession(max_ts_session, min_ts_session):
            max_ts_session_datetime = datetime.utcfromtimestamp(max_ts_session / 1000)
            min_ts_session_datetime = datetime.utcfromtimestamp(min_ts_session / 1000)
            spent_time_session = max_ts_session_datetime - min_ts_session_datetime
            result = spent_time_session.total_seconds()/3600

            return result

        calculate_time_inSession_udf = udf(calculate_time_inSession, FloatType())
        spent_time_df = spent_time_df.withColumn("time_spent_Session_hour", calculate_time_inSession_udf(spent_time_df.max_ts_session, spent_time_df.min_ts_session))

        w = Window.partitionBy(spent_time_df.userId)
        spent_time_df = spent_time_df.withColumn("time_spent_Total_hour", sum(spent_time_df.time_spent_Session_hour).over(w))
        spent_time_df = spent_time_df.drop('max_ts_session','min_ts_session')
        spent_time_df = spent_time_df.withColumn('time_spent_Session_hour', round(col('time_spent_Session_hour')/1, 2))
        spent_time_df = spent_time_df.withColumn('time_spent_Total_hour', round(col('time_spent_Total_hour')/1, 2))
        spent_time_df_only_total = spent_time_df.drop('time_spent_Session_hour','sessionId').distinct()
        
        # Make df_new for machine learning
        df_new = df.select("userId",'gender', 'churn', 'os_system', 'location_first', 'total_sessionId', 'total_itemInSession', \
        "time_after_id_creation_hour", "total_Top100_artist_alltime", "total_Top100_song_week").dropna().drop_duplicates()
        df_new = df_new.join(thumbsup_df, 'userId', how='left').distinct()
        df_new = df_new.join(thumbsdown_df, 'userId', how='left').distinct()
        df_new = df_new.join(advert_df, 'userId', how='left').distinct()
        df_new = df_new.join(addfriend_df, 'userId', how='left').distinct()
        df_new = df_new.join(addplaylist_df, 'userId', how='left').distinct()
        df_new = df_new.join(sub_upgrade_df, 'userId', how='left').distinct()
        df_new = df_new.join(sub_downgrade_df, 'userId', how='left').distinct()
        df_new = df_new.join(error_df, 'userId', how='left').distinct()
        df_new = df_new.join(logout_df, 'userId', how='left').distinct()
        df_new = df_new.fillna(0, subset=['total_thumbsup','total_thumbsdown', 'total_rolladvert', 'total_addfriend', 'total_addplaylist',\
        'total_sub_upgrade', 'total_sub_downgrade', 'total_error', 'total_logout'])
        df_new = df_new.join(last_level_df, 'userId', how='left').distinct()
        df_new = df_new.join(spent_time_df_only_total, 'userId', how='left').distinct()
        
        df_new1 = df_new.withColumn("time_spent_Total_day", round(col('time_spent_Total_hour')/24, 2))
        df_new1 = df_new1.withColumn("time_after_id_creation_day", round(col('time_after_id_creation_hour')/24, 2))
        df_new1 = df_new1.withColumn("avg_total_sessionId_afterCreation", round(col('total_sessionId') / col('time_after_id_creation_day'), 2))
        df_new1 = df_new1.withColumn("avg_itemInSession_afterCreation", round(col('total_itemInSession')/col('time_after_id_creation_day'), 2))
        df_new1 = df_new1.withColumn("avg_thumbsup_afterCreation", round(col('total_thumbsup')/col('time_after_id_creation_day'), 2))
        df_new1 = df_new1.withColumn("avg_thumbsdown_afterCreation", round((col('total_thumbsdown')/col('time_after_id_creation_day')), 2))
        df_new1 = df_new1.withColumn("avg_rolladvert_afterCreation", round((col('total_rolladvert')/col('time_after_id_creation_day')), 2))
        df_new1 = df_new1.withColumn("avg_addfriend_afterCreation", round((col('total_addfriend')/col('time_after_id_creation_day')), 2))
        df_new1 = df_new1.withColumn("avg_addplaylist_afterCreation", round((col('total_addplaylist')/col('time_after_id_creation_day')), 2))
        df_new1 = df_new1.withColumn("avg_error_afterCreation", round((col('total_error')/col('time_after_id_creation_day')), 2))
        df_new1 = df_new1.withColumn("avg_logout_afterCreation", round((col('total_logout')/col('time_after_id_creation_day')), 2))
        df_new1 = df_new1.withColumn("avg_total_Top100_artist_alltime", round((col('total_Top100_artist_alltime')/col('time_after_id_creation_day')), 2))
        df_new1 = df_new1.withColumn("avg_total_Top100_song_week", round((col('total_Top100_song_week')/col('time_after_id_creation_day')), 2))

        
        return df_new1


    df = subfunc_cleaning(df)
    print("## Cleaning is done ##")
    df = subfunc_featureExtraction(df)
    print("## Feature extraction is done ##")
    df_new1 = subfunc_joinDf_featureExtraction(df)
    print("## Joining dataframes and additional feature extraction are done ##")

    return df_new1

