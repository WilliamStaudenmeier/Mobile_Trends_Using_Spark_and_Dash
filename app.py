"""
This is the main app and entry point that calls modules from the controller and processor asyncronously
"""


import plotly.graph_objects as go  # or plotly.express as px
import processor
from processor import firstsFig1, distFig1, releasedFig1, ratingsFig1, biggestsFig1
import dash
from dash import dcc
import dash_html_components as html

app = dash.Dash()
app.layout = html.Div(
    [
        html.H1(
            children="PlayStore and AppStore Trends - Using Spark and Dash",
            style={"textAlign": "center"},
        ),
        dcc.Graph(figure=distFig1),
        dcc.Graph(figure=ratingsFig1),
        dcc.Graph(figure=releasedFig1),
        dcc.Graph(figure=firstsFig1),
        dcc.Graph(figure=biggestsFig1),
    ]
)

app.run_server(debug=True, use_reloader=False)  # Turn off reloader if inside Jupyter
