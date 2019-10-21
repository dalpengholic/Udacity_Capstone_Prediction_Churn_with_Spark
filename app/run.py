import json
import plotly
import pandas as pd
from datetime import datetime
import findspark
findspark.init()

from flask import Flask
from flask import render_template, request
import plotly.graph_objs as go
import plotly.express as px

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml import PipelineModel, Pipeline
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField, SelectField, FloatField
from wtforms.widgets import html5


app = Flask(__name__)

# Initiate SparkSession
spark = SparkSession.builder \
    .master('local') \
    .appName('sparkify') \
    .getOrCreate()

# load model
model_gbt = PipelineModel.load("../model/sparkify_model")

# Load dataframe
df_ML = spark.read.parquet("../model/sparkify.parquet")
df_pd = df_ML.toPandas()  
location_list = df_pd.location_first.unique().tolist()
location_list = [ (location,location) for location in location_list]

# Graph 1
graph1 = df_pd.groupby('gender')['churn'].value_counts(normalize=True).unstack()
g1_name1 = str(graph1.columns[0])
g1_name2 = str(graph1.columns[1])
g1_x = graph1.index
g1_y1 = graph1.values.T[0]
g1_y2 = graph1.values.T[1]
# Graph 2
graph2 =df_pd.groupby('os_system')['churn'].value_counts(normalize=True).unstack()
g2_name1 = str(graph2.columns[0])
g2_name2 = str(graph2.columns[1])
g2_x = graph2.index
g2_y1 = graph2.values.T[0]
g2_y2 = graph2.values.T[1]

# Graph 3
graph3 = df_pd.groupby('last_level')['churn'].value_counts(normalize=True).unstack()
g3_name1 = str(graph3.columns[0])
g3_name2 = str(graph3.columns[1])
g3_x = graph3.index
g3_y1 = graph3.values.T[0]
g3_y2 = graph3.values.T[1]

# Graph 4
g4_y1 = df_pd[df_pd.churn==0]['time_after_id_creation_day']
g4_y2 = df_pd[df_pd.churn==1]['time_after_id_creation_day']


# Graph 5
g5_y1 = df_pd[df_pd.churn==0]['avg_total_Top100_artist_alltime']
g5_y2 = df_pd[df_pd.churn==1]['avg_total_Top100_artist_alltime']

# Graph 6
g6_y1 = df_pd[df_pd.churn==0]['avg_total_Top100_song_week']
g6_y2 = df_pd[df_pd.churn==1]['avg_total_Top100_song_week']


# index webpage displays cool visuals 
@app.route('/')
@app.route('/index')
def index():
    graphs = [
        {
            'data': [
                go.Bar(name=g1_name1+" (not churn)",
                    x=g1_x,
                    y=g1_y1
                ),
                go.Bar(name=g1_name2+" (churn)",
                    x=g1_x,
                    y=g1_y2
                )
            ],

            'layout': {
                'title': 'Churn Rate By Gender',
                'yaxis': {
                    'title': "Rate"
                },
                'xaxis': {
                    'title': "Gender"
                },
                'legend' : {'x':0.8, 'y':1}
                


            }
        },
        
        {
            'data': [
                go.Bar(name=g2_name1+" (not churn)",
                    x=g2_x,
                    y=g2_y1
                ),
                go.Bar(name=g2_name2+" (churn)",
                    x=g2_x,
                    y=g2_y2
                ),    
            ],
            
            'layout': {
                'title': 'Churn Rate By Operating System',
                'yaxis': {
                    'title' : 'Rate'
                },
                'xaxis': {
                    'title' : 'Operating system',
                },
                'legend' : {'x':0.8, 'y':1}
            }
        },
        {
            'data': [
                go.Bar(name=g3_name1+" (not churn)",
                    x=g3_x,
                    y=g3_y1
                ),
                go.Bar(name=g3_name2+" (churn)",
                    x=g3_x,
                    y=g3_y2
                ),    
            ],
            
            'layout': {
                'title': 'Churn Rate By Last Level',
                'yaxis': {
                    'title' : 'Rate'
                },
                'xaxis': {
                    'title' : 'Last level',
                },
                'legend' : {'x':0.8, 'y':1}
            }
        },
         {
            'data': [
                go.Box(name=g2_name1+" (not churn)",
                
                    y=g4_y1
                ),
                go.Box(name=g2_name2+" (churn)",
                    
                    y=g4_y2
                ),    
            ],
            
            'layout': {
                'title': 'Active Days After Id Creation',
                'yaxis': {
                    'title' : 'Number of Days'
                },
                'xaxis': {
                    'title' : 'Churn',
                },
                'legend' : {'x':0.8, 'y':1}
            }
        },



        {
            'data': [
                go.Box(name=g2_name1+" (not churn)",
                
                    y=g5_y1
                ),
                go.Box(name=g2_name2+" (churn)",
                    
                    y=g5_y2
                ),    
            ],
            
            'layout': {
                'title': 'Number of Listening: All-time Top100 Artists',
                'yaxis': {
                    'title' : 'Number of listening per day'
                },
                'xaxis': {
                    'title' : 'Churn',
                },
                'legend' : {'x':0.8, 'y':1}
            }
        },
        {
            'data': [
                go.Box(name=g2_name1+" (not churn)",
                
                    y=g6_y1
                ),
                go.Box(name=g2_name2+" (churn)",
                    
                    y=g6_y2
                ),    
            ],
            
            'layout': {
                'title': 'Number of Listening: This Week Top100 songs',
                'yaxis': {
                    'title' : 'Number of listening per day'
                },
                'xaxis': {
                    'title' : 'Churn',
                },
                'legend' : {'x':0.8, 'y':1}
            }
        },
    
        ]

    # encode plotly graphs in JSON
    ids = ["graph-{}".format(i) for i, _ in enumerate(graphs)]
    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)

    # render web page 
    return render_template('index.html', ids=ids, graphJSON=graphJSON)


# class to make form on prediction page
class userinfoForm(Form):
    gender = SelectField('gender', choices=[('M','Male'),('F','Female')],validators=[validators.required()])
    level = SelectField('last_level', choices=[('paid','Paid'),('free','Free')],validators=[validators.required()])
    location = SelectField('location', choices=location_list, validators=[validators.required()])
    active_day = FloatField('active_day', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_sessionId = FloatField('avg_sessionId', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_itemInSession = FloatField('avg_itemInSession', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_thumbsup = FloatField('avg_thumbsup', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_thumbsdown = FloatField('avg_thumbsdown', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_advert = FloatField('avg_advert', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_addfriend = FloatField('avg_addfriend', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_addplaylist = FloatField('avg_addplaylist', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_error = FloatField('avg_error', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_logout = FloatField('avg_logout', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_alltime_Top100_artist = FloatField('avg_alltime_Top100_artist', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    avg_thisweek_Top100_song = FloatField('avg_thisweek_Top100_song', validators=[validators.required()],widget=html5.NumberInput(step=0.1))
    submit = SubmitField('Submit to predict', validators=[validators.required()])


# web page that handles user query and displays model results
@app.route('/prediction', methods=['GET', 'POST'])
def prediction():
    form = userinfoForm(request.form)
    if request.method =='GET':
        return render_template('prediction.html', form=form)
    else :
        gender = request.form['gender']
        level = request.form['level']
        location = request.form['location']
        active_day = request.form['active_day']
        avg_sessionId = request.form['avg_sessionId']
        avg_itemInSession = request.form['avg_itemInSession']
        avg_thumbsup = request.form['avg_thumbsup']
        avg_thumbsdown = request.form['avg_thumbsdown']
        avg_advert = request.form['avg_advert']
        avg_addfriend = request.form['avg_addfriend']
        avg_addplaylist = request.form['avg_addplaylist']
        avg_error = request.form['avg_error']
        avg_logout = request.form['avg_logout']
        avg_alltime_Top100_artist = request.form['avg_alltime_Top100_artist']
        avg_thisweek_Top100_song = request.form['avg_thisweek_Top100_song']

            # get spark context
        sc = SparkContext.getOrCreate()
        
        # create spark dataframe to predict customer churn using the model
        df_user = sc.parallelize([[gender, level, location, active_day, avg_sessionId, avg_itemInSession, avg_thumbsup, avg_thumbsdown, avg_advert, avg_addfriend, avg_addplaylist, avg_error, avg_logout, avg_alltime_Top100_artist, avg_thisweek_Top100_song]]).\
        toDF(['gender', 'last_level', 'location_first', 'time_after_id_creation_day',\
        'avg_total_sessionId_afterCreation', 'avg_itemInSession_afterCreation','avg_thumbsup_afterCreation',\
        'avg_thumbsdown_afterCreation','avg_rolladvert_afterCreation',\
        'avg_addfriend_afterCreation','avg_addplaylist_afterCreation','avg_error_afterCreation','avg_logout_afterCreation',\
        'avg_total_Top100_artist_alltime','avg_total_Top100_song_week'])

        # set correct data types
        cast_list = ['time_after_id_creation_day','avg_total_sessionId_afterCreation', 'avg_itemInSession_afterCreation','avg_thumbsup_afterCreation',\
        'avg_thumbsdown_afterCreation','avg_rolladvert_afterCreation',\
        'avg_addfriend_afterCreation','avg_addplaylist_afterCreation','avg_error_afterCreation','avg_logout_afterCreation',\
        'avg_total_Top100_artist_alltime','avg_total_Top100_song_week']
        for i in cast_list:
            df_user = df_user.withColumn(i, df_user[i].cast(DoubleType()))

        user_prediction = model_gbt.transform(df_user)
        
        decision = user_prediction.select(user_prediction.prediction).collect()[0]["prediction"]
        churn_probability = round(user_prediction.select(user_prediction.probability).collect()[0]["probability"][1],2)
        if decision == 0:
            decision = 'NO'
            wordForSparkify = 'The user is not going to churn'

        else:
            decision = 'YES'
            wordForSparkify = 'The user is going to churn. We need to do some actions or promotions for the user.'

        return render_template('result.html', decision=decision, churn_probability=churn_probability, wordForSparkify=wordForSparkify)

 
def main():
    app.run(host='0.0.0.0', port=3001, debug=True)


if __name__ == '__main__':
    main()