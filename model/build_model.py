import findspark
findspark.init()

import sys
from func_Create_spark import create_sparkSession
from func_Build_model import build_model
from func_Save_model import save_model


def main():
    if len(sys.argv) == 3:
        df_filepath, model_filepath = sys.argv[1:]
        spark = create_sparkSession()
        print("## SparkSession is created ##")  
        df_ML = spark.read.parquet(df_filepath)
        print("## Parquet data is loaded ##")
        model_gbt = build_model(df_ML)
        print("## Model of GBTClassifier is trained ##")
        save_model(spark, model_gbt, model_filepath)
        print("## Model is saved ##")  

    else:
        print('Please provide the filepath of the Sparkify log data '\
        'as the first argument and the filepath of the trained model to '\
        'save the model to as the second argument. \n\nExample: python '\
        'build_model.py ./sparkify.parquet ./sparkify_model')


if __name__ == '__main__':
    main()