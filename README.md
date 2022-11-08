# Mobile Trends Using Spark_and_Dash

This is a light-weight Dash app using Python and Spark on the backend to asyncronously process large files of PlayStore and AppStore data.  

The app consists of three main files:
- app.py (entrypoint)
- processor.py (processes data into figures)
- controller.py (loads and cleans data files)

The application runs on Python 3.8.  It will not run on Python3.10 without refactoring the Collections library which no longer includes Iterable as of 3.10.

- python3 -m pip install --user virtualenv (unless you already have venv or virtualenv installed)
- python3.8 -m venv env 
- source env/bin/activate
- pip install -r requirements.txt
- python app.py

The app will take five minutes to extract and process the data, and will run locally on http://127.0.0.1:8050/.

To run tests:

- pytest tests.py


