
#Data Wrangling libraries
import numpy as np
import pandas as pd

#Datetime wrangling libraries
from dateutil.parser import parse
from datetime import datetime
import datetime as dt

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func,inspect
from sqlalchemy import distinct

# Web app libraries

from flask import Flask,jsonify

###############################################################

# DATA BASE SET UP

###############################################################

# create engine
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

###############################################################

# FLASK SET UP

###############################################################

app=Flask(__name__)


###############################################################

# FLASK ROUTES

###############################################################

@app.route("/")

def Home():
    print("Server received request for 'Home'")
    return (f"Welcome to my Home Page<br/>"
            f"Available Routes:<br/>"
            f"/api/v1.0/precipitation/'date'<br/>"
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/tobs<br/>"
            f"/api/v1.0/'start'<br/>"
            f"/api/v1.0/'start'/'end'<br/>")

@app.route("/api/v1.0/precipitation/<date>")

def Precipitation(date):

    "Prefered year format YYYY-mm-dd"

    # Parse the date parameter
    
    mD=dt.datetime.strptime(date, '%Y-%m-%d')

    # Calculate the date 1 year ago from the last data point in the database

    twelve_months = mD - dt.timedelta(days=365)

    # Design a query to retrieve the last 12 months of precipitation data and plot the results
    # Perform a query to retrieve the data and precipitation scores

    twlvMonths=session.query(Measurement.date,Measurement.prcp).\
                                        filter(Measurement.date >= twelve_months).\
                                                        order_by(Measurement.date.desc()).all()

    dfTwlv=pd.DataFrame(twlvMonths,columns=('Date','Precipitation'))
    dfTwlv.set_index('Date',inplace=True)
    dfTwlv.sort_values(by='Date',inplace=True)
    print("Server received request for 'Precipitation'")
    return jsonify(dfTwlv.to_dict(orient='dict'))

@app.route("/api/v1.0/stations")

def Stations():

    stations=session.query(Measurement.station,func.count(Measurement.station)).\
                                                    group_by(Measurement.station).\
                                                            order_by(func.count(Measurement.station).desc()).all()

    dfStations=pd.DataFrame(stations,columns=('Station','ObsCount'))
    #dfStations.set_index('Station',inplace=True)

    print("Server received request for 'Stations'")
    return jsonify(dfStations.to_dict(orient='dict'))

@app.route("/api/v1.0/tobs")

def Tobs():

    # retrive max date in the data set

    tobsmaxDate= session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    # parse the max Date point of the data set.

    tobsmD=dt.datetime.strptime(tobsmaxDate[0], '%Y-%m-%d')

    # Calculate the date 1 year ago from the last data point in the database
    tobstwelve_months = tobsmD - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores

    tobstwlvMonths=session.query(Measurement.date,Measurement.prcp).\
                                    filter(Measurement.date >= tobstwelve_months).\
                                                        order_by(Measurement.date).all()

    # Save the query results as a Pandas DataFrame and set the index to the date column

    tobsdfTwlv=pd.DataFrame(tobstwlvMonths,columns=('Date','Precipitation'))
    tobsdfTwlv=tobsdfTwlv.set_index('Date')

    print("Server received request for 'tobs'")

    return jsonify(tobsdfTwlv.to_dict(orient='dict'))


@app.route("/api/v1.0/<startDate>")

def statsStart(startDate):

    "Prefered year format YYYY-mm-dd"

    # Parse the start parameter
    
    strtDt=dt.datetime.strptime(startDate, '%Y-%m-%d')


    sdQuery=session.query(func.min(Measurement.tobs),func.avg(Measurement.tobs),func.max(Measurement.tobs)).\
                                                                                    filter(Measurement.date >= strtDt).all()
    dfsdQuery=pd.DataFrame(sdQuery,columns=('TMin','TAvg','TMax'))
    

    print("Server received request for 'start'")

    return jsonify(dfsdQuery.to_dict(orient='dict'))

@app.route("/api/v1.0/<start>/<end>")

def statsStartEnd(start,end):

    "Prefered year format YYYY-mm-dd"

    # Parse the start parameter
    
    startDate=dt.datetime.strptime(start, '%Y-%m-%d')
    endDate=dt.datetime.strptime(end, '%Y-%m-%d')


    startEndTobs=session.query(func.min(Measurement.tobs),func.avg(Measurement.tobs),func.max(Measurement.tobs)).\
                                                                                    filter(Measurement.date >= startDate).\
                                                                                        filter(Measurement.date <= endDate).all()

    dfstartEndTobs=pd.DataFrame(startEndTobs,columns=('TMin','TAvg','TMax'))
    

    print("Server received request for 'start-end'")

    return jsonify(dfstartEndTobs.to_dict(orient='dict'))

if __name__=="__main__":

    app.run(debug=True)




