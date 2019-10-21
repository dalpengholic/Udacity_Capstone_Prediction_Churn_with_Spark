# Udacity Data Scientist Nanodegree program
## Capstone Project_Prediction Model for Sparkify

## Table of Contents
1. [About the Project](#about_the_project)
2. [How to Use](#how_to_use)
    1. [Dependency](#dependency)
    2. [Installation](#installation)
    3. [Run](#run)
3. [File Structure](#file_structure)
4. [Results](#results)
    1. [Web app](#web_app)
    2. [Things to be improved](#things_to_be_improved)
5. [License](#license)
6. [Acknowledgements](#acknowledgements)

<a name="about_the_project"></a>

## About the Project
This is the capstone project of 2nd term of Data Science Nanodegree Program by Udacity. The goal of the project is to create a prediction model to find Sparkify users who are going to churn. Sparkify is a virtual music streaming service like Spotify. The major components of the project consists of three parts.

1. ETL Pipeline : Original log file of Sparkify given by Udacity is loaded in a dataframe of Apache Spark dataframe and preprocessed(extraction, transformation, and loading) to make a refined dataframe for a machine learning pipeline.
2. ML pipeline : Prediction model is built and trained with the dataframe from ETL pipeline to predict potential churn users.
3. Flask Web App : A web app contains the visual summary of a dataset used training a model and a prediction page that an user of this web can input a basic data of a Sparkify user and get a prediction result.

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
https://github.com/dalpengholic/Udacity_Capstone_Prediction_Churn_with_Spark.git
<a name="run"></a>
### Run

1. Run the following commands in the project's root directory to set up your database and model.
- Unzip 'mini_sparkify_event_data.json.zip'

- To run ETL pipeline that cleans original log data and saves it as a parquet type
`python model/create_df.py mini_sparkify_event_data.json model/sparkify.parquet`

- To run ML pipeline that trains model and saves it
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
│   ├── func_Save_model.py
├── notebooks
│   ├── Sparkify on IBM Watson.ipynb
│   ├── Sparkify-organizing_applyScaler_makeModel.ipynb
│   ├── Sparkify-organizing_feature_extraction.ipynb
│   ├── Sparkify_Submission.ipynb
│   ├── Sparkify_data_check.ipynb
├── LICENSE
├── README.md
```
<a name="results"></a>
## Results
<a name="web_app"></a>
### Web App
1. Main page of the web app. It contains the six plots, which are the summary of statistic of a training dataset. 
The first plot shows the distribution of message genre. The second plot shows frequency of categories
![main page](https://github.com/dalpengholic/Udacity_Disaster_response_project/blob/master/sample/main.png)

2. Result of input new message on the web app. When a user input a new message about disaster, the trained model classifies where the message belongs to.
![search page](https://github.com/dalpengholic/Udacity_Disaster_response_project/blob/master/sample/search.png)
<a name="things_to_be_improved"></a>
### Things to be improved
The model used in the web app was trained with an imbalanced dataset. For example, the training dataset did not have any messages beloning 'child alone' category. In addition, messages about categories like 'offer', 'fire', and 'hospitals' were also rare. One solution for this imbalanced case is to add new messages related to minority categories to reduce imbalance. The other workaround solution could be using other libraries such as 'imbalanced-learn' for advanced sampling methods like SMOTE (Synthetic Minority Over-Sampling Technique) and ADASYN (Adaptive Synthetic sampling approach). 
<a name="license"></a>
## License
This source code is made available under the [MIT License](https://github.com/dalpengholic/Udacity_Disaster_response_project/blob/master/LICENSE).
<a name="acknowledgements"></a>
## Acknowledgements
This project is given by the result of collaboration between [Udacity](https://www.udacity.com) and [Figure Eight](https://www.figure-eight.com)