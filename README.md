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
Having the right metrics is critical for companies in any industries because it directly shows the signal of the health of its business. Churn rate is one of the metrics used a lot for companies having SaaS(software as a service) model. So, for Sparkify, a music streaming service like Spotify, it is natural to consider its churn rate as a main metric. As [the article](https://hackernoon.com/defining-churn-the-right-way-5396722ddb96) mentioned, the metrics like acquisition and conversion are more important for a company selling furniture because buying furniture is not a repetitive action within a short time frame.

[The same article](https://hackernoon.com/defining-churn-the-right-way-5396722ddb96) commented on the general definition of churn rate that no critical event made by a user in a certain time frame. The formula of churn rate is given that the users remained at the end of a certain time frame divided by the users at the beginning of a certain time frame. The definition of churn rate of this Sparkify project can be calculated that the number of users who submitted churn decision to Sparkify service divided by total users in the given sample dataset by Udacity.

As mentioned in [this article](https://hbr.org/2014/10/the-value-of-keeping-the-right-customers), churn rate could be used not only to anticipate future profit, but also to provide opportunities to do marketing for the users who have the potential to churn, to enhance the main service of the Sparkify platform, and to do right promotion to the meaningful groups of people who will stay in Sparkify more than other groups.

In sum, it is beneficial for Sparkify to have a model that predicts which users prefer to churn.

### Metrics for Model
The both of `F1 score` and `recall` are selected as the main metrics for this project. As the log dataset is imbalanced, which is the number of users to stay is about four times larger than that of users to churn, it is not recommended to use accuracy as a metric. For example, a model only can predict '0'(not churn), it results in 80% of accuracy, but 0% of F1, Precision, and Recall. As I left comments about the metrics in conclusion, I thought that the main goal of this project is to predict Sparkify users who are going to churn so that Sparkify will execute some actions not to lose their customers. It is fact that the total cost of promotion for potential churn users could be cheaper than that of cost of promotion to get new users. So, it is better to do promotion for a predicted group by low precision and high recall model (consisting of most of the potential churn users and some users to stay) than for a predicted group by the opposite model, which has high precision but low recall.


<a name="analysis"></a>
## Analysis
### Data Exploration and Data Visualization
It is possible to see the both data exploration and visualization in the main notebook `Sparkify_Submission.ipynb` and the web-app main page
### Input Data
- artist: Name of artist of the song played, ex)Adam Lambert
- auth: Status of user such as Logged in or Logged out ex) Logged in
- firstName: First name of user, ex) Colin
- gender: Gender of user, ex) M
- itemInSession: Number of item in a session, ex)80
- lastName: Last name of user, ex) Freeman
- length: Length of song, ex) 277.89016
- level: Subscription status of user,  ex) paid
- location: Geographical information where user connect, ex) Bakersfield, CA
- method: REST API, ex) PUT
- page: Page that user at the event ex) NextSong
- registration: Unique number like userId, ex) 1.538173e+12
- sessionId: Number of session id, ex) 8
- song: Name of song, ex) Rockpools
- status: HTTP response status codes, ex) 200
- ts: Timestamp of event, ex) 1538352117000
- userAgent: Agent information of user, ex) Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) G..
- userId: UserId number, ex) 30

## Methodology
### Steps of Preprocessing
The preprocessing (ETL) to create a dataframe for ML pipeline as follows:
1. Remove rows that `userId` column has only whitespace
2. Make list of customer ids who churned
3. Create `os-system` column from userAgent column
4. Extract first two letters to make as a `location_first` column from location column
5. Create `total number of session ID` column
6. Create `total number of itemInSession` column
7. Create `last_access_time`,`first_access_tim`e columns
8. Create `Active time after id creation` column
9. Convert `timestamp` column to `number of week, month, year` columns
10. Create `number of listening of all-time top 100 artists` column
11. Create `number of listening of This week top 100 songs` column
12. Create dataframes to make columns of numbers of `thumbsup`, `thumbsdown`, `advert`, `addfriend`, `addplaylist`, `sub_upgrade`, `sub_downgrade`, `error`, `logout`, `last_level`, `spent_time` by joining

### Complication during the project
I wanted to make a user similarity matrix to create a new feature by clustering. It was pretty fast and straightforward to make the matrix using Pandas and to do clustering using scikit-learn. However, it was too slow to make the matrix using PySpark. I tried the processes on my virtual environment given by Udacity and my local machine. I wondered this processes could have been completed faster if I had done it on AWS or IBM cluster environment.

<a name="Conclusion"></a>
## Conclusion
### Results of F1 Score and Recall
At local machine
- F1 score (test/validation set): 0.81 / 0.91
- Recall (test/validiation set: 0.78 / 0.5

At IBM Watson flatform
- F1 score (test/validation set): 0.86 / 0.82
- Recall (test/validiation set: 0.56 / 0.46

### Intersting Part
It was amazing for me to learn how the log data of customers was structured and how real companies do ETL to extract meaningful information from it. The project also gave me motivation to learn a new language like Scala which is the langauge to make Spark. Though Apache spark provides three APIs JAVA, Python, Scala, I read an [article](https://mindfulmachines.io/blog/2018/6/apache-spark-scala-vs-java-v-python-vs-r-vs-sql26) that the performance of Scala was better in some cases.

### Difficult Parts
1. Difference of dataframe Pandas vs Spark
- Reason: Two different concepts of two dataframe from Pandas and Spark. It was hard for me to get the concept of lazy execution of Spark even though I finished all extracurriculum of Spark given by Udacity. Now, I can describe the difference analogously. Any methods used for manipulation of dataframe in Spark except actions like the actions like `collect()`,`show()` are the same as just drawing a blue print what Spark will execute when any action is triggered. It was ok when I wrote some lines of code in a cell and executed it. However, when I had to start a jupyter notebook kernel multiple times, it was too slow to execute all cells because I had not commented any actions in the cells. I should have commented all actions
- Solution : I commented all actions in the cells above the current cell that I was.

2. Make a Flask web-app
- Reason: 
Even though, I referred to the Flask wep-app that I made before, it was hard to make it because I had a very few limitied knowledge about front-end and back-end. Simply, I had no idea of how Bootstrap and Flask work when I finished making a prediction model.
- Solution: I took small online courses for learning the basic of Bootstrap and Flask to make my web-app.

### Reflection
#### Importance of metric

According to [the article](https://hackernoon.com/defining-churn-the-right-way-5396722ddb), critical events is defined that main actions executed on a certain platform by customers. Customers solve their own problems by doing a series of critical events. Therefore, it is critical to analyze what customers mostly do on a platform and to understand the result of critical events could bring the satisfaction of the customers or the increase of the happiness of the customers. They will stay when they are happy, otherwise they will go away. To know the pros and cons of the main service on a certain platform, it is recommended to analyze the user group who has stayed in more than others. After the analysis, a service provider could know which parts of a service give satisfaction to customer or not and enhance their service quality. In addition, well-analyzed information about the loyal group shows which groups must be target groups in the future marketing for acquisition of new users. 

### Improvement
1. Model
- User similarity matrix for segmenation
I wished to make a user similarity matrix using a Pyspark dataframe to create a group feature to use in clustering. I planned to use the result of clustering for the prediction model to make it better. I made a matrix, but it was impractical because it needed a lot of time to execute. So, I had to give up the plan. 

2. GridSearch
The hyperparameters of GBTclassifier were not optimized.


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
This project is given by [Udacity](https://www.udacity.com). 
