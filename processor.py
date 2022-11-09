import time
x = time.time()
import sys
import asyncio
import datetime as dt
import pandas as pd
from pyspark.sql import SparkSession
import databricks.koalas as ks
from databricks.koalas import option_context
import plotly.express as px
import plotly.graph_objects as go
import controller
from controller import final

distFig1 = None
releasedFig1 = None
ratingsFig1 = None
biggestsFig1 = None
firstsFig1 = None


async def distributions():
	## Creating box plot for distribution - this is the longest and heaviest process
	global distFig1
	with option_context("compute.ops_on_diff_frames", True, "compute.default_index_type", 'distributed'):
		try:
			data =final[['Size_Bytes','Category']].to_pandas()#.sort_values(by='Size_Bytes')
		except:
			print('Size is null')
		distFig1 = px.box(data, x="Category", y="Size_Bytes", color = 'Category',  title = "Distribution by Category - Double Click on Legend to Isolate Trace" )


async def firsts():
	## Reducing dataset to first time releases by app and then counting
	global firstsFig1
	with option_context("compute.ops_on_diff_frames", True, "compute.default_index_type", 'distributed'):
		try:
			data = final.groupby(['Category','App_Name'])['Released'].min().to_frame('First_Released').reset_index()
		except:
			print("Released is null")
		data = data[['App_Name','First_Released','Category']].drop_duplicates()
		data = data.groupby(['First_Released', 'Category']).size().to_frame('Count').reset_index().sort_values(by='First_Released').to_pandas()
		firstsFig1 = px.line(data, x='First_Released', y = 'Count', color = 'Category', title = 'First Releases by App - Time Series')


async def releases():
	## Counting based off release column - could include re-releases
	global releasedFig1
	with option_context("compute.ops_on_diff_frames", True, "compute.default_index_type", 'distributed'):
		try:
			data = final.groupby(['Released', 'Category']).size().to_frame('New_Releases').reset_index().sort_values(by='Released').to_pandas()
		except:
			print('New Releases are null')
		releasedFig1 = px.line(data, x='Released', y = 'New_Releases', color = 'Category', title = 'New Releases - Time Series')


async def ratings():
	## Created weighted score for ratings and transforming
        global ratingsFig1
        with option_context("compute.ops_on_diff_frames", True, "compute.default_index_type", 'distributed'):
                try:
                        score = final[['Category','Average_User_Rating','Reviews']]
                        score['Reviews']=score['Reviews'].astype(float)
                        score['Average_User_Rating']=score['Average_User_Rating'].astype(float)
                        score['Weighted_Rating']=score['Average_User_Rating']*score['Reviews']
                except:
                        print("Score is null")
                temp = score.groupby(['Category'])['Reviews'].sum().to_frame('Reviews').reset_index().to_pandas()
                score = score.groupby(['Category'])['Weighted_Rating'].sum().to_frame('Weighted_Rating').reset_index().to_pandas()
                score=score.merge(temp, on = 'Category')
                score['Average_Rating'] = score['Weighted_Rating']/score['Reviews']
                ratingsFig1 = px.bar(score, x='Category', y = 'Average_Rating',color = 'Category',text_auto='4s',  title = 'Average Rating by Category')

async def biggests():
	## Creating reduced dataset of top apps by size, category, and year
        global biggestsFig1
        with option_context("compute.ops_on_diff_frames", True, "compute.default_index_type", 'distributed'):
                try:
                        bigs = final[['Category', 'Released_Year', 'Size_Bytes', 'App_Name']]
                        bigs['Rank'] = bigs.groupby(['Released_Year','Category'])['Size_Bytes'].rank('dense',ascending = False)
                        bigs = bigs[bigs['Rank']<=10]
                except:
                        print("Table is null")
                bigs = bigs[['App_Name','Category', 'Released_Year','Size_Bytes', 'Rank']].sort_values(by=['Category','Released_Year','Rank']).to_pandas()
                biggestsFig1 = go.Figure(data=[go.Table(header=dict(values=list(bigs.columns)),
                 cells=dict(values=[bigs.App_Name, bigs.Category,bigs.Released_Year, bigs.Size_Bytes, bigs.Rank]))])
                biggestsFig1.update_layout(title='Top Apps By Size, Category, and Year')

loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.gather(
    distributions(),
    releases(),
    ratings(),
    biggests(),
    firsts()
)) 
	
y = time.time()

print(y-x)
