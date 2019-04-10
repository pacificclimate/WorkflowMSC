from pycds import Obs, History, Variable
from sqlalchemy.sql import func
from sqlalchemy import and_, or_
from query_helpers import total_days, total_years, count, min_time, max_time

def query_annual_rain(start_time, end_time, session):
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
    years = total_years(start_time, end_time)
    sums = Obs.datum/years
    annual_rain = func.sum(sums).label("annual_rain")

    completeness = (count(Obs)/total_days(start_time, end_time)).label('completeness')

    # construct desired table
    query = (
             session.query(annual_rain,
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
                    .filter(and_(Variable.standard_name == 'thickness_of_rainfall_amount',
                                 Variable.cell_method == 'time: sum'))
                    .filter(or_(Variable.name == '10',
                                Variable.name == '48'))
                    .group_by(History.lat,
                              History.lon,
                              History.station_id)
             )

    return query

def query_annual_precip(start_time, end_time, session):
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
    years = total_years(start_time, end_time)
    sums = Obs.datum/years
    annual_precip = func.sum(sums).label("annual_precip")

    completeness = (count(Obs)/total_days(start_time, end_time)).label('completeness')

    query = (
             session.query(annual_precip,
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
