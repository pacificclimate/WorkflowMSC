from pycds import Obs, History, Variable
from sqlalchemy.sql import func
from sqlalchemy import and_, or_
from query_helpers import total_days, total_years, count, min_time, max_time


def query_rain_rate_15(start_time, end_time, session):
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
    days = total_days(start_time, end_time)
    years = total_years(start_time, end_time)

    completeness = (count(Obs)/(days/years)).label("completeness")

    query = (
             session.query(rainfall_rate,
                           min_time(Obs),
                           max_time(Obs),
                           History.lat,
                           History.lon,
                           History.station_id,
                           completeness)
                    .select_from(Obs)
                    .join(Variable, Obs.vars_id == Variable.id)
                    .join(History, Obs.history_id == History.id)
                    .filter(and_(Obs.time >= start_time,
                                 Obs.time < end_time))
                    .filter(Variable.standard_name == 'rainfall_rate')
                    .filter(Variable.name == '127')
                    .group_by(func.extract("year", Obs.time),
                              History.lat,
                              History.lon,
                              History.station_id,
                              )
             )

    return query

def rrall(start_time, end_time, session):
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
    days = total_days(start_time, end_time)
    years = total_years(start_time, end_time)

    #completeness = (count(Obs)/(days/years)).label("completeness")

    query = (
             session.query(Obs.datum,
                           Obs.time,
                           History.lat,
                           History.lon,
                           History.station_id)
                    .select_from(Obs)
                    .join(Variable, Obs.vars_id == Variable.id)
                    .join(History, Obs.history_id == History.id)
                    .filter(and_(Obs.time >= start_time,
                                 Obs.time < end_time))
                    .filter(Variable.standard_name == 'rainfall_rate')
                    .filter(Variable.name == '127')
                    .group_by(Obs,
                              History.lat,
                              History.lon,
                              History.station_id,
                              )
             )

    return query


def has_rain(start_time, end_time, session):

    query = (
             session.query(Obs.datum,
                           Obs.time,
                           History.lat,
                           History.lon,
                           History.station_id)
                    .select_from(Obs)
                    .join(Variable, Obs.vars_id == Variable.id)
                    .join(History, Obs.history_id == History.id)
                    .filter(and_(Obs.time >= start_time,
                                 Obs.time < end_time))
                    .filter(or_(Variable.name == '48',
                                Variable.name == '10'))
                    .group_by(Obs,
                              History.lat,
                              History.lon,
                              History.station_id,
                              )
             )

    return query
