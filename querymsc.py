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
from lmoments3 import distr


from pycds import *


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
        rainfall_rate = func.max(Obs.datum).label("rainfall_rate")

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