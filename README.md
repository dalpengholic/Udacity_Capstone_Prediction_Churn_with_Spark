# Udacity Data Scientist Nanodegree program

## Capstone Project: Prediction Model for Sparkify

## Table of Contents
1. [Project Definition](#project_definition)
2. [Analysis](#analysis)
3. [Conclusion](#conclusion)
4. [How to use](#how_to_use)    
    1. [Dependency](#dependency)
    2. [Installation](#installation)
    3. [Run](#run)
5. [File Structure](#file_structure)
6. [Sample Result](#sample_result)
7. [License](#license)
8. [Acknowledgements](#acknowledgements)




<a name="project_definition"></a>
## Project Definition
### Project Overview
This is the capstone project of 2nd term of Data Science Nanodegree Program by Udacity. The goal of the project is to create a prediction model to find Sparkify users who are going to churn. Sparkify is a virtual music streaming service like Spotify. The major components of the project consists of three parts.

1. ETL Pipeline : Original log file of Sparkify given by Udacity is loaded in a dataframe of Apache Spark dataframe and preprocessed(extraction, transformation, and loading) to make a refined dataframe for a machine learning pipeline.
2. ML pipeline : Prediction model is built and trained with the dataframe from ETL pipeline to predict potential churn users.
3. Flask Web App : A web app contains the visual summary of a dataset used training a model and a prediction page that an user of this web can input a basic data of a Sparkify user and get a prediction result.

### Problem Statement

### Metrics

<a name="analysis"></a>
## Analysis
### Data Exploration
#### Input Data
### Data Visualization

<a name="Conclusion"></a>
## Conclusion
### Reflection
### Improvement



<a name="how_to_use"></a>
## How to Use

<a name="dependency"></a>
### Dependency
The code should run with no issues using Python versions 3.* with the libararies as follows.
- Numpy, Pandas, Pyspark, findspark for ETL and ML pipelines.
- Flask, Plotly, wtforms for Flask web app.

<a name="installation"></a>
### Installation
Clone the repositor below.

`https://github.com/dalpengholic/Udacity_Capstone_Prediction_Churn_with_Spark.git`
<a name="run"></a>
### Run

1. Run the following commands in the project's root directory to set up your database and model.
    1. Unzip 'mini_sparkify_event_data.json.zip'

    2. To run ETL pipeline that cleans original log data and saves it as a parquet type
    `python model/create_df.py mini_sparkify_event_data.json model/sparkify.parquet`

    3. To run ML pipeline that trains model and saves it
    `python models/build_model.py model/sparkify.parquet model/sparkify_model`


2. Run the following command in the app's directory to run your web app.
`python run.py`

3. Open http://0.0.0.0:3001/ 
        
<a name="file_structure"></a>
## File Structure
```
├── app
│   ├── run.py
│   └── templates
│       ├── base.html
│       └── index.html
│       └── prediction.html
│       └── result.html
├── model
│   ├── create_df.py
│   └── build_model.py
│   ├── func_Build_model.py
│   └── func_Create_spark.py
│   ├── func_ETL.py
│   └── func_Load.py
│   └── func_Save_model.py
├── notebooks
│   ├── Sparkify on IBM Watson.ipynb
│   ├── Sparkify-organizing_applyScaler_makeModel.ipynb
│   ├── Sparkify-organizing_feature_extraction.ipynb
│   ├── Sparkify_Submission.ipynb
│   └── Sparkify_data_check.ipynb
├── sample
│   └── sample_webapp.gif
├── LICENSE
├── README.md
```


<a name="sample_result"></a>
## Sample Result
Main page of the web app. 
1. It contains the six plots, which are the summary of statistic of a training dataset. 
2. Click `prediction` in the navbar if you want to predict a user having certain input data.
3. Input data and click the submit button below.

![index page](https://github.com/dalpengholic/Udacity_Capstone_Prediction_Churn_with_Spark/blob/master/sample/sample_webapp.gif)

<a name="license"></a>
## License
This source code is made available under the [MIT License](https://github.com/dalpengholic/Udacity_Capstone_Prediction_Churn_with_Spark/blob/master/LICENSE).

<a name="acknowledgements"></a>
## Acknowledgements
This project is given by [Udacity](https://www.udacity.com) 
