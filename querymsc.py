import datetime
from calendar import monthrange

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_
from sqlalchemy.sql import func
from sqlalchemy import case

import numpy as np
import math
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

        # query variables
        self.month = self.safe_month(month)
        self.start_time = start_time
        self.end_time = end_time

        self.total_days = float((end_time - start_time).days)

        # year range 
        yr_interval = float(end_time.year - start_time.year)
        self.yr_interval = self.annual_safe_year(yr_interval)

        self.days_in_month = float(monthrange(self.start_time.year, month)[1])*yr_interval

        # total number of hours in month
        self.total_hours = self.days_in_month*24

        # record the max and min time taken from the database
        self.time_max = func.max(Obs.time).label("time_max")
        self.time_min = func.min(Obs.time).label("time_min")

        # record lat/lon/station_id from history of the station
        self.lat = History.lat.label("lat")
        self.lon = History.lon.label("lon")
        self.station_id = History.station_id.label("station_id")

        # count the observations used in calculation percentile
        self.count = func.count(Obs.datum).label('obs_count')

        # create a condition that separates daily and hourly data and calculates
        # the completeness based on total number of theoretical observations
        self.daily_complete = (self.count/self.total_days).label('completeness')
        self.hourly_complete = (self.count/self.total_hours).label('completeness')
        self.hourly_complete_yr = (self.count/((self.total_days/self.yr_interval)*24*4)).label('completeness')
        self.month_days_complete = (self.count/self.days_in_month).label('completeness')

        #self.annual_complete = (self.count/self.total_days_ann).label('completeness')
        
    def safe_month(self, month):
        if (month > 12) or (month < 1):
            raise ValueError("Invalid month specified.")

        return month

    def annual_safe_year(self, yr_interval):
        print(yr_interval)
        # check start and end times
        if (self.start_time < self.end_time) is False:
            raise ValueError("Start time cannot be later than end time.")

        #if (yr_interval) < 1.0:
        #    raise ValueError("Annual rainfall value requires \
        #                     a time window of at least one year.")
        #    return None

        return yr_interval

    def query_annual_rain(self, session):
        """A query to get the total annual average rainfall amount 
        at a given station over a range of years.
        This method uses daily total rainfall.
        -----------------------------------------
        Args:
            session (sqlalchemy Session): session constructed using
                connection string and engine
        Returns: 
            query (sqlalchemy query): sqlalchemy query object 
                for design value
        """



        annual_rain = func.sum(Obs.datum*0.1/self.yr_interval).label("annual_rain")

        # construct desired table
        query = (
                 session.query(annual_rain,
                               self.time_min,
                               self.time_max,
                               self.lat,
                               self.lon,
                               self.station_id,
                               self.daily_complete)
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(and_(Variable.standard_name == 'thickness_of_rainfall_amount',
                                     Variable.cell_method == 'time: sum'))
                        .filter(or_(Variable.name == '10',
                                    Variable.name == '48'))
                        .group_by(History.lat,
                                  History.lon,
                                  History.station_id)
                 )

        return query

    def query_annual_precip(self, session):
        """A query to get the total annual average precipitation amount 
        at a given station over a range of years. This method uses 
        daily total precipitation.  
        -----------------------------------------
        Args:
            session (sqlalchemy Session): session constructed using
                connection string and engine
        Returns: 
            query (sqlalchemy query): sqlalchemy query object 
                for design value
        """

        annual_precip = func.sum(Obs.datum*0.1/self.yr_interval).label("annual_precip")
    
        query = (
                 session.query(annual_precip,
                               self.time_min,
                               self.time_max,
                               self.lat,
                               self.lon,
                               self.station_id,
                               self.count,
                               self.daily_complete)
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(and_(Variable.standard_name 
                                == 'lwe_thickness_of_precipitation_amount',
                                     Variable.cell_method == 'time: sum'))
                        .filter(or_(Variable.name == '12',
                                    Variable.name == '50'))
                        .group_by(History.lat,
                                  History.lon,
                                  History.station_id)
                 )

        return query
    
    def query_design_temp_percentile(self, session, percentile=0.01, month=1):
        """A query to get the percentile of minimum daily air_temperatures 
        at a station in a given time frame. Daily minimum air
        temperatures are used.
        -----------------------------------------
        Args:
            session (sqlalchemy Session): session constructed using
                connection string and engine
            percentile (float): desire percentile in fraction
            month (int, optional): desired month in which to calculate 
                the desired percentile, default is 1, or January.
        Returns: 
            query (sqlalchemy query): sqlalchemy query object 
                for design value
        """

        month = self.safe_month(month)

        p = (
             (func.percentile_cont(percentile)
                 .within_group(Obs.datum.asc())*0.1)
                 .label("air_temperature")   
             )

        query = (
                 session.query(p,
                               self.time_min,
                               self.time_max,
                               self.lat,
                               self.lon,
                               self.station_id,
                               self.month_days_complete)
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(func.extract("month", Obs.time) == month)
                        .filter(Variable.name == '2')
                        .filter(and_(Variable.standard_name == 'air_temperature',
                                     Variable.cell_method == 'time: minimum'))
                        .filter(Obs.datum != 0.0) # bad obs are sometimes 0.0
                        .group_by(History.lat, 
                                  History.lon, 
                                  History.station_id)
                 )
  
        return query

    def query_design_temp_dry(self, session, month=7, percentile=0.025):
        """A query to get the percentile of maximum daily 
        air_temperatures (it is assumed that dry bulb temperatures are
        identical to regular air temperatures) at a station in a 
        given time frame. Daily maximum air temperatures are used.
        ---------------------------------------------------------
        Args:
            session (sqlalchemy Session): session constructed using
                connection string and engine
            percentile (float, optional): desire percentile in fraction.
                Default value is 0.0025 or the 2.5th percentile
            month (int, optional): desired month in which to calculate 
                the desired percentile, default is 7, or July.
        Returns: 
            query (sqlalchemy query): sqlalchemy query object 
                for design value
        """

        month = self.safe_month(month)

        # get percentile from group convert to celsius
        p = (
             (func.percentile_cont(percentile) 
                  .within_group(Obs.datum.asc())*0.1)
                  .label("wet_bulb_temperature")
             )

        # construct query table
        query = (
                 session.query(p, 
                               self.time_min,
                               self.time_max,
                               self.lat,
                               self.lon,
                               self.station_id,
                               self.month_days_complete)
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(func.extract("month", Obs.time) == month)
                        .filter(and_(Variable.standard_name == 'air_temperature', 
                                     Variable.cell_method == 'time: maximum'))
                        .filter(Variable.name == '1')                  
                        .filter(Obs.datum != 0.0)   # bad obs are sometimes 0.0
                        .group_by(History.lat, 
                                  History.lon, 
                                  History.station_id)
                 )

        return query

    def query_design_temp_wet(self, session, month=7, percentile=0.025):
        """A query to get the percentile of maximum daily 
        wet_bulb_temperatures at a station in a given time frame. 
        There is a mixed frequency of this variable in dbmsc, and so a 
        temp patch to guess the frequency based on obs_count
        is used. As of Feb 28, 2019, Faron Anslow is working to include a
        filtering option in net_vars_id. This is a known issue. 
        ---------------------------------------------------------
        Args:
            session (sqlalchemy Session): session constructed using
                connection string and engine
            percentile (float, optional): desire percentile in fraction.
                Default value is 0.0025 or the 2.5th percentile
            month (int, optional): desired month in which to calculate 
                the desired percentile, default is 7, or July.
        Returns: 
            query (sqlalchemy query): sqlalchemy query object 
                for design value
        """
        month = self.safe_month(month)

        # create a condition that separates daily and hourly data and calculates
        # the completeness based on total number of theoretical observations
        # by guessing the frequency

        expr = (
                case([(func.count(Obs.datum) <= self.days_in_month,
                       self.month_days_complete)], 
                       else_=self.hourly_complete).label('completeness')
                )
        
        # get percentile from group convert to celsius
        p = (
             (func.percentile_cont(percentile) 
                  .within_group(Obs.datum.asc())*0.1)
                  .label("wet_bulb_temp")
             )

        query = (
                 session.query(p,
                               self.time_min,
                               self.time_max,
                               self.lat,
                               self.lon,
                               self.station_id,
                               expr,
                               self.count)
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(func.extract("month", Obs.time) == month)
                        .filter(Variable.name == '79')
                        .filter(Variable.standard_name == 'wet_bulb_temperature')
                        .filter(Obs.datum != 0.0) # bad obs are sometimes 0.0
                        .group_by(History.lat, 
                                  History.lon, 
                                  History.station_id)
                 )

        return query

    def query_variables(self, session):
        """A simple query to get variable types to 
        reference while building workflow tool. 
        -----------------------------------------
        Args:
            session (sqlalchemy Session): session constructed using
                connection string and engine
        Returns: 
            query (sqlalchemy query): sqlalchemy query object 
                for design value
        """

        query = session.query(Variable.id,
                              Variable.standard_name,
                              Variable.description,
                              Variable.unit,
                              Variable.cell_method,
                              Variable.name)

        return query

    def query_deg_day_below_18(self, session):
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

        # get heating degree days below 18 C, convert to celsius, take mean
        hdd = func.sum((18.0-Obs.datum*0.1)/self.yr_interval).label("hdd")
        query = (
                 session.query(hdd,
                               self.time_min,
                               self.time_max,
                               self.lat, 
                               self.lon,
                               self.station_id,
                               self.daily_complete
                               )
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(Variable.name == '3')
                        .filter(and_(Variable.standard_name == 'air_temperature',
                                     Variable.cell_method == 'time: mean'))
                        .filter(Obs.datum != 0.0) # bad obs are sometimes 0.0
                        .group_by(History.lat,
                                  History.lon,
                                  History.station_id)
                 )
  
        return query

    def query_rain_rate_15(self, session):
        """A query to get the maximum annual 15 minute duration 
        rainfall amounts. This method uses quarter hour 
        observatons and extracts the maximum at a 
        given station in a given year.
        -----------------------------------------
        Returns:
            query (sqlalchemy query): sqlalchemy query object
            containing hdd values
        """

        # get max 15 minute rainfall rate
        rainfall_rate = func.max(Obs.datum*0.1).label("rainfall_rate")

        query = (
                 session.query(rainfall_rate,
                               self.time_min,
                               self.time_max,
                               self.lat, 
                               self.lon,
                               self.station_id,
                               (func.count(Obs.datum)/(self.total_days/self.yr_interval)/(24*4)).label('completeness'),
                               History.freq)
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(Variable.standard_name == 'lwe_thickness_of_precipitation_amount')
                        .filter(or_(Variable.name == '263',
                                    Variable.name == '264',
                                    Variable.name == '265',
                                    Variable.name == '266'))
                        .group_by(func.extract("year", Obs.time), 
                                  History.lat, 
                                  History.lon,
                                  History.station_id,
                                  History.freq)
                 )
  
        return query

    def query_rain_rate_15_rfr_test(self, session):
        """A query to get the maximum annual 15 minute duration 
        rainfall amounts. This method uses the rainfall_rate
        variable which is a daily maximum. This method returns 
        the maximum annual 15 minute duration rainfall amounts.
        -----------------------------------------
        Returns:
            query (sqlalchemy query): sqlalchemy query object
            containing hdd values
        """

        # get annual max 15 minute rainfall rate
        rainfall_rate = func.max(Obs.datum*0.1).label("rainfall_rate")
        print(self.yr_interval)
        query = (session.query(rainfall_rate,
                               self.time_min,
                               self.time_max,
                               self.lat, 
                               self.lon,
                               self.station_id,
                               (func.count(Obs.datum)/(self.total_days/self.yr_interval)).label('completeness'),
                               History.freq)
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(Variable.standard_name == 'rainfall_rate')
                        .filter(Variable.name == '127')
                        .group_by(func.extract("year", Obs.time), 
                                  History.lat, 
                                  History.lon,
                                  History.station_id,
                                  History.freq))
        return query

    def query_rain_rate_one_day_1_50(self, session):
        """A query to get the maximum annual 24hr duration 
        rainfall amounts.
        -----------------------------------------
        Returns:
            query (sqlalchemy query): sqlalchemy query object
            containing hdd values
        """

        # get annual max 24hr duration rainfall rate
        rainfall_rate = func.max(Obs.datum*0.1).label("rainfall_rate")

        query = (
                 session.query(rainfall_rate,
                               History.elevation,
                               self.time_min,
                               self.time_max,
                               self.lat, 
                               self.lon,
                               self.station_id,
                               (func.count(Obs.datum)/(self.total_days/self.yr_interval)).label('completeness'))
                        .select_from(Obs)
                        .join(Variable, Obs.vars_id == Variable.id)
                        .join(History, Obs.history_id == History.id)
                        .filter(and_(Obs.time >= self.start_time,
                                     Obs.time < self.end_time))
                        .filter(and_(Variable.standard_name == 'rainfall_rate',
                                     Variable.name == '161'))
                        .group_by(func.extract("year", Obs.time), 
                                  History.elevation,
                                  History.lat, 
                                  History.lon,
                                  History.station_id)
                 )
 
        return query