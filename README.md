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
6. [Sample Web Application](#sample_web_application)
7. [License](#license)
8. [Acknowledgements](#acknowledgements)


<a name="project_definition"></a>
## Project Definition
### Project Overview
This is the capstone project of 2nd term of Data Science Nanodegree Program by Udacity. The goal of the project is to create a prediction model by using Pyspark to find Sparkify users who are going to churn. Sparkify is a virtual music streaming service like Spotify. The major components of the project consists of the three parts as follows.

1. ETL Pipeline : Original log file of Sparkify given by Udacity is loaded in a dataframe of Apache Spark dataframe and preprocessed(extraction, transformation, and loading) to make a dataframe for a machine learning pipeline.
2. ML pipeline : Prediction model is built and trained with the dataframe from ETL pipeline to predict potential churn users.
3. Flask Web App : A web app contains the visual summary of a dataset used training a model and a prediction page that an user of this web can input a basic data of a Sparkify user and get a prediction result.

### Problem Statement
Metrics are important because all types of business make decision based on their metriccs. If metrics were set incorrectly, decisions from the metrics go wrong


### Metrics
The both of `F1 score` and `recall` are selected as the main metrics for this project. As the log dataset is imbalanced, which is the number of users to stay is about four times larger than that of users to churn, it is not recommended to use accuracy as a metric. For example, a model only can predict '0'(not churn), it results in 80% of accuracy, but 0% of F1, Precision, and Recall. As I left comments about the metrics in conclusion, I thought that the main goal of this project is to predict Sparkify users who are going to churn so that Sparkify will execute some actions not to lose their customers. It is fact that the total cost of promotion for potential churn users could be cheaper than that of cost of promotion to get new users. So, it is better to do promotion for a predicted group by low precision and high recall model (consisting of most of the potential churn users and some users to stay) than for a predicted group by the opposite model, which has high precision but low recall.


<a name="analysis"></a>
## Analysis
### Data Exploration and Data Visualization
It is possible to see the both data exploration and visualization in the main notebook `Sparkify_Submission.ipynb` and the web-app main page
### Input Data
artist: Name of artist of the song played, ex)Adam Lambert
auth: Status of user such as Logged in or Logged out ex) Logged in
firstName: First name of user, ex) Colin
gender: Gender of user, ex) M
itemInSession: Number of item in a session, ex)80
lastName: Last name of user, ex) Freeman
length: Length of song, ex) 277.89016
level: Subscription status of user,  ex) paid
location: Geographical information where user connect, ex) Bakersfield, CA
method: REST API, ex) PUT
page: Page that user at the event ex) NextSong
registration: Unique number like userId, ex) 1.538173e+12
sessionId: Number of session id, ex) 8
song: Name of song, ex) Rockpools
status: HTTP response status codes, ex) 200
ts: Timestamp of event, ex) 1538352117000
userAgent: Agent information of user, ex) Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) G..
userId: UserId number, ex) 30


<a name="Conclusion"></a>
## Conclusion
### Reflection
1. Difference of dataframe Pandas vs Spark
2. Metric, threshold
### Improvement
1. Model
- user similarity matrix,segmentation
- threshold, pipeline
- interval, critical event, promotion fee for current users and new users
- Sample number
2. Web App
- threshold adjustment
- data plots

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


<a name="sample_web_application"></a>
## Sample Web Application
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
