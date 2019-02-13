import datetime
import calendar

from pycds import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
from sqlalchemy.sql import func

import numpy as np
import pandas as pd

class WorkflowMSC:
    def __init__():
        print("Initiated!")

    def baseline(start_time, end_time):
        """
        A simple query to get all observations for all available 
        stations in a given time window
        --------------------------------------------------------
        In: datetime object. start_time and end_time specify the time window
        Out: sqlalchemy query object
        """
        query = session.query(Obs.datum, Obs.time, Variable.standard_name, History.station_id).filter(Obs.time <= end_time).filter(Obs.time >= start_time).join(History).join(Variable)
        return query

    def precip_query(start_time, end_time):
        """
        A simple query to get all precipitation observations of total precip for 
        all available stations in a given time window
        ------------------------------------------------------------------------
        In: datetime object. start_time and end_time specify the time window
        Out: sqlalchemy query object
        """
        query = baseline(start_time, end_time).filter(Variable.standard_name == "lwe_thickness_of_precipitation_amount").filter(or_(Variable.description == "Total precipiation", Variable.id == 1397))
        return query

    def annual_rain(start_year, end_year):
        """
        A query to get the total rainfall amount at a given station over a range
        of years 
        ------------------------------------------------------------------------
        In: datetime object. start_time and end_time specify the time window. Must be >= 1
        Out: sqlalchemy query object
        """
        if start_year.year - end_year.year < 1:
            raise ValueError("Annual precipitation value requires a time window of at least 1 year.")

        yr_interval = float(np.abs(end_year.year - start_year.year))
        print(yr_interval)
        query = session.query(func.sum(Obs.datum*0.1/yr_interval).label("sum"), func.max(Obs.time).label("max_date"), History.lat, History.lon, History.station_id).group_by(History).having(func.max(Obs.time) >= end_year).filter(Variable.standard_name == "lwe_thickness_of_precipitation_amount").filter(or_(Variable.description == "Total precipiation", Variable.id == 1397))
        query = query.filter(Obs.time <= end_year).filter(Obs.time >= start_year).join(History).join(Variable)
        return query
    
    def baseline(start_time, end_time):
        query = session.query(Obs.datum, Obs.time, Variable.standard_name, History.station_id).filter(Obs.time <= end_time).filter(Obs.time >= start_time).join(History).join(Variable)
        return query

    def precip_query(start_time, end_time):
        query = baseline(start_time, end_time).filter(Variable.standard_name == "lwe_thickness_of_precipitation_amount").filter(or_(Variable.description == "Total precipiation", Variable.id == 1397))
        return query

    def design_temp_25(start_time, end_time, month = 1):

        query = session.query(func.percentile_cont(0.025).within_group(
                        Obs.datum.asc()).label("temp"), 
                        func.min(Obs.time).label("time"),
                        History.lat,
                        History.lon,
                        History.station_id).group_by(History)\
                        .filter(or_(Variable.description == "Air temperature", Variable.id == 1510)) \
        .filter(Variable.standard_name == "air_temperature") \
        .filter(and_(func.extract("month", Obs.time) == month, func.extract("year", Obs.time) <= end_time.year)) \
        .filter(func.extract("year", Obs.time) >= start_time.year)

        return query

    def design_temp_1(start_time, end_time, month = 1):

        query = session.query(func.percentile_cont(0.01).within_group(
                        Obs.datum.asc()).label("temp"), 
                        func.min(Obs.time).label("time"),
                        History.lat,
                        History.lon,
                        History.station_id).group_by(History) \
        .filter(or_(Variable.description == "Air temperature", Variable.id == 1510)) \
        .filter(Variable.standard_name == "air_temperature") \
        .filter(and_(func.extract("month", Obs.time) == month, func.extract("year", Obs.time) <= end_time.year)) \
        .filter(func.extract("year", Obs.time) >= start_time.year)

        return query

    def show_design_values():

        query = session.query(Variable.standard_name, Variable.description, Variable.unit, Variable.id)

        return query