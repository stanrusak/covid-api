from fastapi import FastAPI, HTTPException
from fastapi.params import Body
from pydantic import BaseModel
import pandas as pd
from typing import List
from .functions import *
import os

DATA_PATH = '../data/covid-19-data/public/data'
FILE_NAME = 'owid-covid-data.csv'

app = FastAPI()

from fastapi.middleware.cors import CORSMiddleware

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def load_data() -> None:

    data_path = os.path.abspath(DATA_PATH)
    data_file = FILE_NAME
    data = pd.read_csv(os.path.join(data_path, data_file))

    df = data.set_index(["date", "location"]).unstack()
    df.columns = df.columns.swaplevel(0, 1)
    df.sort_index(axis=1, level=0, inplace=True)
    
    return df

data = load_data()

class Request(BaseModel):

    location: str | List = None 
    country: str | List = None
    start: str = None
    end: str = None
    period: str = None
    columns: list = None

@app.get("/")
def root():

    return {
        "info": "An API serving the global COVID-19 data collected by the good people of Our World in Data. Disclaimer: I am not affiliated with OWID.",
        "project url": "",
        "original data": "https://github.com/owid/covid-19-data/tree/master/public/data",
        "Our World in Data": "https://ourworldindata.org/coronavirus",
        "metadata": 
            {
                "period": f"{min(data.index)}-{max(data.index)}",
                "days": len(data),
                "locations": len(data.columns.get_level_values(0).unique()),
                "columns": len(data.columns.get_level_values(1).unique())
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
        over continents and world as a whole. Same as query `countries`.
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

@app.get("/countries")
def get_countries():
    """ 
        Returns a list of locations present in the database.
        Includes countries, select territories as well as aggregates
        over continents and world as a whole. Same as query `locations`.
    """
    return get_locations()

@app.get("/countries/{location}")
def get_country(location):
    return get_location(location)

@app.get("/iso")
def iso():
    """
        Returns a list of country ISO codes
    """
    iso_codes = data.xs("iso_code", axis=1, level=1).iloc[-1].dropna().to_dict()
    return {"ISO Codes": iso_codes}

@app.post("/")
def root(request: Request):

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
    
    return result
    
@app.get("/countries/{metric}/top/{num}")
def rank(metric: str, num: int):
    """ Returns top {num} countries ranked by total cases/deaths per million{metric}"""

    if metric == "cases":
        column = "total_cases_per_million"
    elif metric == "deaths":
        column = "total_deaths_per_million"
    else:
        raise HTTPException(status_code=404, detail=f"Invalid route {metric}. Valid routes are 'cases' and 'deaths'.")
    