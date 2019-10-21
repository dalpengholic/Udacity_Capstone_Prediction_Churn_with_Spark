def save_model(spark, model_gbt, model_filepath):
    '''
    Function saves model
    
    Input:
    1. spark: Pyspark session
    2. model_gbt : prediction model
    3. model_filepath: filepath to save model
    '''
    model_gbt.write().overwrite().save(model_filepath)