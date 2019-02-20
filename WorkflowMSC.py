import datetime
import calendar

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
from sqlalchemy.sql import func
import numpy as np
import pandas as pd
import pgpasslib

from pycds import *

class WorkflowMSC:
    """The work flow tool helps query the Meteorlogical 
    Services Canada database at PCIC using ORM to return
    design values used in the national building code project.
    
    Attributes:
        start_time (datetime object): take all observations after
        end_time (datetime object): take all observations before
        month (int, optional): month to extract design values
            where necessary
    """

    def __init__(self, connection_string, start_time, end_time, month=1):

        self.start_time = start_time
        self.end_time = end_time
        self.month = month
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # if month specified, check
        if (month>12) or (month<1) :
            raise ValueError("Invalid month specified.")

    def baseline(self):
        """A simple query to get all observations for 
        all available stations in a given time window
        --------------------------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query constructed 
                using ORM to get all observations in given time frame
        """
        session = self.session
        query = session.query(Obs.datum, Obs.time, 
                              Variable.standard_name, 
                              History.station_id) \
                              .filter(Obs.time<=self.end_time) \
                              .filter(Obs.time>=self.start_time) \
                              .join(History).join(Variable)
        return query

    def precip_query(self):
        """A simple query to get all precipitation observations
        of total precip for all available stations in a 
        given time window
        --------------------------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query constructed 
                using ORM
        """
        query = self.baseline().filter(Variable.standard_name=="lwe_thickness_of_precipitation_amount") \
                               .filter(or_(Variable.description=="Total precipiation", 
                                           Variable.id==1397))
        return query

    def annual_rain(self):
        """A query to get the total annual average rainfall amount 
        at a given station over a range of years. The table contains
        the average total rainfall in mm, the maximum time used to
        calculate the average from the station, the latitude, longitude
        and the station id.  
        -----------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query constructed 
                using ORM with annual average rainfall
        """
        
        yr_interval = float(np.abs(self.end_time.year-self.start_time.year))
        print("Year interval:", yr_interval)
        if (yr_interval) < 1.0:
            raise ValueError("Annual precipitation value requires \
                             a time window of at least one year.")
            return None

        # construct desired table
        session = self.session
        query = session.query(func.sum(Obs.datum*0.1/yr_interval).label("sum"), 
                              func.max(Obs.time).label("max_date"), 
                              History.lat, History.lon, 
                              History.station_id)

        # filter results to annual rain in given time range
        # select stations that have a max observed time that is
        # at least the ending time requested.
        query = query.group_by(History) \
                     .having(func.max(Obs.time)>=self.end_time) \
                     .filter(Variable.standard_name=="lwe_thickness_of_precipitation_amount") \
                     .filter(or_(Variable.description=="Total precipiation", Variable.id==1397)) \
                     .filter(Obs.time<=self.end_time) \
                     .filter(Obs.time>=self.start_time) \
                     .join(History).join(Variable)

        return query
    
    def design_temp_25(self):
        """A query to get the 2.5th percentile of a given month across
        the entire operating history of a station in a range of time. 
        Only the year from start and end times are used to create the
        time frame. All frequencies of observations are used, and the
        regular non-corrected air temperature is being used for this 
        calculation.
        -----------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query constructed 
                using ORM to query temperature percentiles 
        """
        if (self.month>12) or (self.month<1) :
            raise ValueError("Invalid month specified.")
            return None

        # construct query table
        session = self.session
        query = session.query(func.percentile_cont(0.025) \
                              .within_group(Obs.datum.asc()).label("temp"),
                                            func.min(Obs.time).label("time_min"),
                                            func.max(Obs.time).label("time_max"),
                                            History.lat,
                                            History.lon,
                                            History.station_id,
                                            History.station_name)
        # filter query
        query = query.group_by(History) \
                     .filter(and_(Variable.standard_name=='air_temperature',
                                  Variable.id==1510)) \
                     .filter(func.extract("month", Obs.time)==self.month) \
                     .join(History)

        return query

    def design_temp_1(self):
        """A query to get the 1st percentile of a given month across
        the entire operating history of a station in a range of time. 
        Only the year from start and end times are used to create the
        time frame. All frequencies of observations are used, and the
        regular non-corrected air temperature is being used for this 
        calculation.
        -----------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query constructed 
                using ORM to query temperature percentiles 
        """
        if (month>12) or (month<1) :
            raise ValueError("Invalid month specified.")
            return None
        # construct query table
        session = self.session
        query = session.query(func.percentile_cont(0.01) \
                              .within_group(Obs.datum.asc()).label("temp"),
                                            func.min(Obs.time).label("time_min"),
                                            func.max(Obs.time).label("time_max"),
                                            History.lat,
                                            History.lon,
                                            History.station_id,
                                            History.station_name)
        # filter query
        query = query.group_by(History) \
                     .filter(and_(Variable.standard_name=='air_temperature',
                                  Variable.id==1510)) \
                     .filter(func.extract("month", Obs.time)==1) \
                     .join(History)

        return query

    def query_variables(self):
        """A simple query to get all available variables to 
        reference while building workflow tool. 
        -----------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query constructed 
                using ORM to query variables
        """
        session = self.session
        query = session.query(Variable.standard_name,
                              Variable.description,
                              Variable.unit,
                              Variable.id)

        return query