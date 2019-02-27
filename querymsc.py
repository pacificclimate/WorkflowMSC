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

class WorkflowTools:
    """The work flow tool helps query the Meteorlogical 
    Services Canada database at PCIC using ORM to return
    design values used in the national building code project.
    
    Attributes:
        start_time (datetime object): take all observations after
        end_time (datetime object): take all observations before
        month (int, optional): month to extract design values
            where necessary
    """

    def __init__(self, start_time, end_time, month=1):

        self.start_time = start_time
        self.end_time = end_time
        self.month = month

        # if month specified, check
        if (month>12) or (month<1) :
            raise ValueError("Invalid month specified.")

        # check start and end times
        if (start_time < end_time) is False:
            raise ValueError("Start time cannot be later than end time.")

    def query_annual_rain(self, session):
        """A query to get the total annual average rainfall amount 
        at a given station over a range of years. The table contains
        the average total rainfall in mm, the maximum and minimum time used to
        calculate the average from the station, the latitude, longitude
        and the station id.  
        -----------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query object 
                for annual average rainfall
        """
        yr_interval = float(np.abs(self.end_time.year-self.start_time.year))

        if (yr_interval) < 1.0:
            raise ValueError("Annual precipitation value requires \
                             a time window of at least one year.")
            return None

        # construct desired table
        query = (
                 session.query(func.sum(Obs.datum*0.1/yr_interval).label("sum"),
                               func.min(Obs.time).label("min_date"),
                               func.max(Obs.time).label("max_date"),
                               History.lat,
                               History.lon,
                               History.station_id)
                        .join(History)
                        .join(Variable, Variable.id == Variable.id)
                        .having(
                                and_(func.max(Obs.time) >= self.end_time, 
                                     func.min(Obs.time) <= self.start_time)
                                )
                        .filter(
                                and_(Obs.time <= self.end_time,
                                     Obs.time >= self.start_time)
                                )
                        .filter(
                                or_(Variable.id == 1395,
                                    Variable.id == 1452)
                                )
                        .filter(Variable.standard_name == 'thickness_of_rainfall_amount')
                        .group_by(History.lat,
                                  History.lon,
                                  History.station_id)
                )

        return query

    def query_annual_precip(self, session):
        """A query to get the total annual average precipitation amount 
        at a given station over a range of years. The table contains
        the average total rainfall in mm, the maximum and minimum time used to
        calculate the average from the station, the latitude, longitude
        and the station id.  
        -----------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query object 
                for annual average precip
        """
        yr_interval = float(np.abs(self.end_time.year-self.start_time.year))
        print("Year interval:", yr_interval)
        if (yr_interval) < 1.0:
            raise ValueError("Annual precipitation value requires \
                             a time window of at least one year.")
            return None

        # filter results to annual rain in given time range
        # select stations that have a max observed time that is
        # at least the ending time requested
        # construct desired table
        query = (
                 session.query(
                                func.sum(Obs.datum*0.1/yr_interval).label("sum"),
                                func.min(Obs.time).label("min_date"),
                                func.max(Obs.time).label("max_date"),
                                History.lat, History.lon,
                                History.station_id
                               )
                        .join(History)
                        .join(Variable, Variable.id == Variable.id)
                        .having(
                                and_(func.max(Obs.time) >= self.end_time, 
                                     func.min(Obs.time) <= self.start_time)
                                )
                        .filter(
                                and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time)
                                )
                        .filter(Variable.id == 1397)
                        .group_by(History.lat,
                                  History.lon,
                                  History.station_id)
                )

        return query
    
    def query_design_temp_percentile(self, session, percentile=0.01):
        """A query to get the 1st percentile of a given month across
        the entire operating history of a station in a range of time. 
        Only the year from start and end times are used to create the
        time frame. All frequencies of observations are used, and the
        regular non-corrected air temperature is being used for this 
        calculation.
        -----------------------------------------
        Args:
            session (sqlalchemy Session): session constructed using
                connection string and engine
            percentile (float): desire percentile in fraction
        Returns: 
            query (sqlalchemy query): sqlalchemy query constructed 
                using ORM to query temperature percentiles 
        """
        days_in_month = 31
        yr_interval = float(np.abs(self.end_time.year-self.start_time.year))
        total_days = days_in_month*yr_interval        

        # factor of 0.1 added for unit conversion percentile due to
        # msc native units of 0.1 Celsius.
        #
        # construct query filter by time range, and month desired 
        # by percentile filter by Variable 1510 which corresponds
        # to hourly sampled air temperature at given station. 
        #
        # unique standard names to describe the observation/variable
        # desired doesn't exist in dbmsc, and so filtering by 
        # Variable.id is the only way to access this information
        # other than Variable.description.
        # 
        # filter 0.0 because historic stations used a measured air 
        # temperature of precisely 0.0 for bad measurements.
        query = (
                 session.query(
                                (func.percentile_cont(percentile)
                                     .within_group(Obs.datum.asc())*0.1)
                                     .label("temp"),
                                func.min(Obs.time).label("time_min"),
                                func.max(Obs.time).label("time_max"),
                                History.lat.label("lat"),
                                History.lon.label("lon"),
                                History.station_id.label("station_id"),
                                (func.count(Obs.datum)/total_days).label('completeness')
                               )
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(func.extract("month", Obs.time) == self.month)
                        .filter(Variable.name == '2')
                        .filter(and_(Variable.standard_name == 'air_temperature',
                                     Variable.cell_method == 'time: minimum'))
                        .filter(Obs.datum != 0.0)
                        .group_by(History.lat, 
                                  History.lon, 
                                  History.station_id)
                )
  
        return query

    def query_design_temp_25_dry(self, session):
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

        # construct query table
        query = session.query(func.percentile_cont(0.025)
                       .within_group(Obs.datum.desc()).label("dry_temp"),
                          func.min(Obs.time).label("time_min"),
                          func.max(Obs.time).label("time_max"),
                          History.lat,
                          History.lon,
                          History.station_id)

        # filter query
        query = query.group_by(History.lat, History.lon, History.station_id) \
                     .filter(and_(Obs.time >= self.start_time, Obs.time <= self.end_time)) \
                     .filter(func.extract("month", Obs.time) == self.month) \
                     .filter(Variable.id == 1299) \
                     .join(History)

        return query

    def query_design_temp_25_wet(self, session):
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

        # construct query table
        query = session.query(func.percentile_cont(0.025) \
                              .within_group(Obs.datum.desc()).label("wet_temp"),
                                func.min(Obs.time).label("time_min"),
                                func.max(Obs.time).label("time_max"),
                                History.lat,
                                History.lon,
                                History.station_id).join(History).join(Variable)

        # filter query
        query = query.group_by(History.lat, History.lon, History.station_id) \
                     .filter(and_(Obs.time >= self.start_time, Obs.time <= self.end_time)) \
                     .filter(func.extract("month", Obs.time) == self.month) \
                     .filter(Variable.id == 1300)

        return query

    def query_variables(self, session):
        """A simple query to get all available variables to 
        reference while building workflow tool. 
        -----------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query constructed 
                using ORM to query variables
        """

        query = session.query(Variable.id,
                              Variable.standard_name,
                              Variable.description,
                              Variable.unit,
                              Variable.cell_method,
                              Variable.name)

        return query

    def deg_day_below_18_query(self, session):
        """A query to get the heating degree days (hdd) 
        "Degree Days Below 18C". If start/end time
        range is longer than a year, then the average
        degree day across the annual range is used.
        Each comparison is made between 18C and a 
        daily mean temperature.
        -----------------------------------------
        Returns: 
            query (sqlalchemy query): sqlalchemy query object
            containing hdd values   
        """

        yr_interval = float(np.abs(self.end_time.year
                            -self.start_time.year))

        query = session.query(func.sum((18.0-Obs.datum*0.1)/yr_interval).label("hdd"),
                              func.min(Obs.time).label("min_date"),
                              func.max(Obs.time).label("max_date"),
                              History.lat, History.lon,
                              History.station_id)

        query = query.group_by(History.lat, History.lon, History.station_id) \
                     .filter((180.0 - Obs.datum) > 0.0) \
                     .filter(and_(Obs.time <= self.end_time, Obs.time >= self.start_time)) \
                     .having((func.extract("year", func.max(Obs.time))
                              - func.extract("year", func.min(Obs.time))) >= yr_interval) \
                     .filter(and_(Variable.standard_name == 'air_temperature',
                                  Variable.id == 1394)) \
                     .join(History).join(Variable)

        return query

