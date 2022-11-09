import pytest
import os
import subprocess
import pandas as pd
import processor
from processor import final
from processor import firstsFig1, distFig1,releasedFig1,ratingsFig1,biggestsFig1 

def test_size_google():
	df =pd.read_csv('Google-Playstore.csv', chunksize = 100)
	for data in df:
		tester = data
		break
	assert len(data) > 0, "empty google data frame"

def test_size_apple():
        df =pd.read_csv('appleAppData.csv', chunksize = 100)
        for data in df:
                tester = data
                break
        assert len(data) > 0, "empty apple data frame"

def test_controller():
	assert len(final) > 0, "data not loading from controller"

import processor
def test_table():
	assert processor.biggestsFig1 != None, "table not processing"

def test_ratings():
	assert processor.ratingsFig1 != None, "ratings not processing"

def test_dist():
	assert processor.distFig1 != None, "distribution not processing"

def test_released():
	assert processor.releasedFig1 != None, "releases not processing"

def test_first():
	assert processor.firstsFig1 != None, "firsts not processing"



