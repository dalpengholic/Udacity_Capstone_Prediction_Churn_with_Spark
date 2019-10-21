def load_data(spark, event_filepath):
    '''
    Function to load log data of sparkify
    Input: 
    1. ss: sparkSession created from create_sparkSession()
    2. event_filepath : json file of the log of sparkify
    Output:
    1. df: pyspark dataframe from event_filepath
    '''
    df = spark.read.options(header=True).json(event_filepath)
    df.cache()
    return df










