from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi.middleware.cors import CORSMiddleware
from fastapi.params import Body
from pydantic import BaseModel
import pandas as pd
from typing import List
from .functions import *
from datetime import datetime as dt
import os
import sys

#DATA_PATH = '../data/covid-19-data/public/data'
FILE_NAME = 'owid-covid-data.json'
DATA_DIR = "../"# os.path.dirname(os.path.dirname(os.path.realpath( __file__)))
FILE_URL = "https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data.csv?raw=true"
UPDATED = dt.strptime("2022-11-25 00:00", "%Y-%m-%d %H:%M")

app = FastAPI(
    title="COVID19 API ",
    description="An API serving the global COVID-19 data collected by the good people of Our World in Data. See `/docs` for usage. Disclaimer: I am not affiliated with OWID.",
    version="0.1.0")

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def fetch_data():

    global UPDATED

    print(f"{dt.now()}: Fetching latest data...", end="")

    try:
        data = pd.read_csv(FILE_URL, dtype={"tests_units": str})
        #data.to_csv(FILE_NAME, index=False)
    except Exception as e:
        print(e)
        raise Exception("Data fetch failed")
        #data = pd.read_csv(FILE_NAME)

    UPDATED = dt.utcnow()

    print("Done")

    return format_data(data)

def update():

    global data
    data = fetch_data()

data = fetch_data() #load_data(DATA_DIR, FILE_NAME)

print(f"Version: {sys.version}")

@app.on_event("startup")
async def load_schedule_or_create_blank():
    
    try:
        sched = BackgroundScheduler()
        sched.add_job(update,'interval', seconds=60 * 60 * 12)
        sched.start()   
        print("Created Schedule")
    except:
        print("Unable to Create Schedule")

class Request(BaseModel):

    location: str | List[str] = None 
    start: str = None
    end: str = None
    columns: list = None

@app.get("/")
def root():
    """ Info and metadata """

    return {
        "info": "An API serving the global COVID-19 data collected by the good people of Our World in Data. See `/docs` for usage. Disclaimer: I am not affiliated with OWID.",
        "project url": "https://github.com/stanrusak/covid-api",
        "original data": "https://github.com/owid/covid-19-data/tree/master/public/data",
        "Our World in Data": "https://ourworldindata.org/coronavirus",
        "metadata": 
            {
                "period": f"{min(data.index)}-{max(data.index)}",
                "days": len(data),
                "locations": len(data.columns.get_level_values(0).unique()),
                "columns": len(data.columns.get_level_values(1).unique()),
                "updated": UPDATED.strftime("%Y-%m-%d %H:%M UTC")
            }
            }

@app.get("/columns")
def get_columns():
    """ Returns a list of columns present in the database"""
    return {"columns": data.columns.get_level_values(1).unique().to_list()}

@app.get("/locations")
def get_locations():
    """ 
        Returns a list of locations present in the database.
        Includes countries, select territories as well as aggregates
        over continents and world as a whole.
    """
    return {"locations": data.columns.get_level_values(0).unique().to_list()}

@app.get("/locations/{location}")
def get_location(location):

    location = validate_locations(data, location)
    
    # if validator returned a dict then location invalid, return error message
    if isinstance(location, dict):
        return location
    
    location = location[0]
    columns = data.columns.get_level_values(1).unique().to_list()

    return {location: get_location_data(location, data, columns, None, None)}

@app.get("/iso")
def iso():
    """
        Returns a list of country ISO codes
    """
    iso_codes = data.xs("iso_code", axis=1, level=1).iloc[-1].dropna().to_dict()
    return {"ISO Codes": iso_codes}

@app.post("/")
def root(request: Request):
    """ Run a detailed data query with JSON request. Accepted parameters:

        - `location` - Location as a string or a list of locations. If not provided, all locations are returned.
        - `columns` - A list of data columns. See the `/columns` endpoint for available columns. If not provided, all columns are returned.
        - `start` - Starting date for the data in the format YYYY-MM-DD. If not provided, earliest point in the data is used.
        - `end` - Ending date for the data in the format YYYY-MM-MM-MM. If not provided, latest point in the data is used.
    """

    locations = parse_locations(data, request)
    
    # if locations not a list then an error occurred, return error
    if isinstance(locations, dict):
        return locations

    start, end = parse_period(request)
    columns = validate_columns(data, request)

    # if columns not a list then an error occurred, return error.
    if isinstance(columns, dict): 
        return columns

    result = {}
    for location in locations:
        
        location_data = data.loc[slice(start,end)].xs(location, axis=1, level=0)
        location_data = location_data[columns].fillna('null')
        

        d = {'date': location_data.index.to_list()}
        for column in columns:
            d[column] = location_data[column].to_list()
        
        result[location] = d

    return result

@app.get("/latest")
def latest():
    """ Returns the latest figures for total cases and deaths """

    columns = ["iso_code", "total_cases_per_million", "total_deaths_per_million"]
    locations = data.columns.get_level_values(0).unique()

    result = {}
    for location in locations:

        result[location] = data[location][columns].dropna(axis=0).apply(lambda x: x[x.notnull()].values[-1]).to_dict()
        # result[location]["updated"] = data[location][columns].dropna(axis=0).apply(lambda x: x[x.notnull()].index[-1]).max()
    
    return result