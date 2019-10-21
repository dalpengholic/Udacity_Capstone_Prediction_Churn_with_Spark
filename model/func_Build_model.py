import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer,OneHotEncoderEstimator, VectorAssembler, MinMaxScaler
from pyspark.ml.classification import  RandomForestClassifier, LogisticRegression, LinearSVC, NaiveBayes, GBTClassifier
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import min, max, size, collect_set, count, when, col, desc, asc, round, sum

import sys

def build_model(df_ML):
    '''
    Function do the last transform dataframe and build a prediction model
    Input: 
    1. df_ML: Pyspark dataframe from ETL_for_ML(df)
    Output:
    2. model_gbt: Model pridicts which userIds are going to churn
    '''
    
    # Make train, test and validation sets 
    train, test, validation = df_ML.randomSplit([0.7, 0.15, 0.15], seed = 41)

    # Make pipes for pipelines
    column_for_stringindexer_list = ["gender", 'last_level', 'location_first']

    indexers = [StringIndexer(inputCol=column, outputCol="indexed_"+column, handleInvalid='skip') for column in column_for_stringindexer_list]

    encoder = OneHotEncoderEstimator(inputCols=["indexed_gender", "indexed_last_level", "indexed_location_first"],\
    outputCols=["gender_feat", "last_level_feat", "location_feat"],handleInvalid = 'keep')

    input_features = ['gender_feat', 'last_level_feat', 'location_feat', 'time_after_id_creation_day',\
    'avg_total_sessionId_afterCreation', 'avg_itemInSession_afterCreation','avg_thumbsup_afterCreation',\
    'avg_thumbsdown_afterCreation','avg_rolladvert_afterCreation',\
    'avg_addfriend_afterCreation','avg_addplaylist_afterCreation','avg_error_afterCreation','avg_logout_afterCreation',\
    'avg_total_Top100_artist_alltime','avg_total_Top100_song_week']
    assembler = VectorAssembler(inputCols=input_features, outputCol="assmebled_features")
    scaler = MinMaxScaler(inputCol="assmebled_features" , outputCol="features")
    # Initiate GBTClassifier
    gbt = GBTClassifier(featuresCol="features",labelCol="churn", maxIter=5)
    # Create pipeline
    pipeline = Pipeline(stages=[indexers[0], indexers[1], indexers[2], encoder, assembler, scaler, gbt])


    # Fit model
    model_gbt = pipeline.fit(train)

    # Predict churn users
    pred_train_gbt = model_gbt.transform(train)
    pred_test_gbt = model_gbt.transform(test)
    pred_validation_gbt = model_gbt.transform(validation)

    # Create prediction and label objects
    predictionAndLabels_train = pred_train_gbt.rdd.map(lambda x: (float(x.prediction), float(x.churn)))
    predictionAndLabels_test = pred_test_gbt.rdd.map(lambda x: (float(x.prediction), float(x.churn)))
    predictionAndLabels_validation = pred_validation_gbt.rdd.map(lambda x: (float(x.prediction), float(x.churn)))

    # Create MulticlassMetrics objects
    metrics_train = MulticlassMetrics(predictionAndLabels_train)
    metrics_test = MulticlassMetrics(predictionAndLabels_test)
    metrics_validation = MulticlassMetrics(predictionAndLabels_validation)

    # Print results of each dataset
    print("F1 score of training set: ", metrics_train.fMeasure())
    print("Precision(churn case) of training set: ", metrics_train.precision(1))
    print("Recall(churn case) of training set: ", metrics_train.recall(1))
    print(metrics_train.confusionMatrix().toArray())
    print()
    print("F1 score of test set: ", metrics_test.fMeasure())
    print("Precision(churn case) of test set: ", metrics_test.precision(1))
    print("Recall(churn case) of test set: ", metrics_test.recall(1))
    print(metrics_test.confusionMatrix().toArray())
    print()
    print("F1 score of validation set: ", metrics_test.fMeasure())
    print("Precision(churn case) of validation set: ", metrics_test.precision(1))
    print("Recall(churn case) of validation set: ", metrics_test.recall(1))
    print(metrics_validation.confusionMatrix().toArray())
    
    return model_gbt
