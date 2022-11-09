import time
x = time.time()
import sys
import asyncio
import datetime as dt
import pandas as pd
from pyspark.sql import SparkSession
import databricks.koalas as ks
from databricks.koalas import option_context


final = None
data = None

async def googles():
	## Loading google data and cleaning 
	global final
	with option_context(
		"compute.ops_on_diff_frames", True,
		"compute.default_index_type", 'distributed'):
		data =ks.read_csv('Google-Playstore.csv')
	gameList = ['Action', 'Adventure', 'Arcade', 'Board', 'Card', 'Casino', 'Casual', 'Educational', 'Music',
	'Puzzle', 'Racing', 'Role Playing', 'Simulation', 'Sports', 'Strategy', 'Trivia', 'Word']
	try:
		final = data[data['Category'].isin(gameList)]
		music = data[data['Category']=='Music & Audio']
		health=data[data['Category']=='Health & Fitness']
		final['SuperCategory']= 'Games'
		music['SuperCategory']='Music'
		health['SuperCategory']='Health'

		final = ks.concat([final,music,health])

		final=final[['App Name', 'Released', 'Size', 'Rating', 'Rating Count', 'SuperCategory']]
	except:
		print("Failed to load google data")
	## Reducing google data and casting types
	final.columns=['App_Name', 'Released', 'Size_Bytes', 'Average_User_Rating', 'Reviews', 'Category']
	final['Released_Year'] = ks.to_datetime(final['Released'], errors = 'coerce').dt.strftime('%Y')
	final['Released'] = ks.to_datetime(final['Released'], errors = 'coerce').dt.strftime('%Y-%m')
	final['Size_Bytes']=final['Size_Bytes'].str.replace('M','000000').astype(int)

async def apples():
	## Loading apple data and cleaning
	global data
	with option_context(
		"compute.ops_on_diff_frames", True,
		"compute.default_index_type", 'distributed'):
		data =ks.read_csv('appleAppData.csv')

	## Reducing data and casting types
	try:
		data = data[data['Primary_Genre'].isin(['Games','Music','Health'])]
		data['Category']= data['Primary_Genre']
		data = data[['App_Name', 'Released', 'Size_Bytes', 'Average_User_Rating', 'Reviews', 'Category']]
		data['Released_Year']=ks.to_datetime(data['Released'], errors='coerce').dt.strftime('%Y')
		data['Released'] = ks.to_datetime(data['Released'], errors='coerce').dt.strftime('%Y-%m')
		data['Size_Bytes']=data['Size_Bytes'].astype(int)
	except:
		print("Failed to load apple data")


loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.gather(
    googles(),
    apples()
))

try:
	final = ks.concat([final, data])
except:
	print("Failed to concat data")
