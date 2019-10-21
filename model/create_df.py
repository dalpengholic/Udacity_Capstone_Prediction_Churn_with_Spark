# import libraries

import findspark
findspark.init()
import sys
#
from func_Create_spark import create_sparkSession
from func_Load import load_data
from func_ETL import ETL_for_ML

def main():
    if len(sys.argv) == 3:
        event_filepath, df_filepath = sys.argv[1:]
        spark = create_sparkSession()
        print("## SparkSession is created ##") 
        df = load_data(spark, event_filepath)        
        print("## Data is loaded ##")
        df_ML = ETL_for_ML(df)
        print("## ETL for ML is completed ##")   
        df_ML.write.mode('overwrite').parquet(df_filepath)
        print("## Dataframe for ML is saved ##") 
        
        
    else:
        print('Please provide the filepath of the Sparkify log data '\
        'as the first argument and the filepath of the trained model to '\
        'save the model to as the second argument. \n\nExample: python '\
        'create_dfForML.py ./mini_sparkify_event_data.json ./sparkify.parquet')


if __name__ == '__main__':
    main()



