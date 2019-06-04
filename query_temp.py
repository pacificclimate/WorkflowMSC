from sqlalchemy.sql import func
from sqlalchemy.orm import aliased
from sqlalchemy import and_, or_, case, distinct
from sqlalchemy import (Table, Column, String, Integer,
                        MetaData, select, func, join)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from calendar import monthrange
from pycds import Obs, History, Variable

from query_helpers import total_days, total_years, count, min_time, max_time, days_in_month, hours_in_month

def temp_quantile(
    session, start_time, end_time,
    method='lower', cell_method='time: minimum',
    net_var_name='2', standard_name='air_temperature',
    quantile=0.01, min_years=8
    ):

    '''A funcion that returns a query of daily temperatures quantiles
    at a station for a given month containing eligible data.
    Eligible means a station has min_years or more fully complete
    years of data for that month. Parameter names follow PyCDS
    meteorological naming conventions found here
    https://data.pacificclimate.org/portal/docs/pycds.html
    -----------------------------------------
    Args:
        session (sqlalchemy Session): session constructed using
            connection string and engine.
        method (str): instructions to calculate the upper or
            lower percentile of the distribution.
        cell_method (str): parameter describing
            variable acquisition method.
        net_var_name (str): network unique variable name for the
            meterological service of canada's database.
        standard_name (str): plain language non unique standard name
            of variable.
        quantile (float): desired percentile level to
            calculate from observations.
        min_years (int): minimum years with fully complete
            record to use in calculations.
    Returns:
        query (sqlalchemy query): sqlalchemy query object
            for design value.
    '''

    # get the number of days in the month of the start_time
    days = monthrange(start_time.year, start_time.month)[1]

    # calculate completeness between expected observations and actual obs
    completeness = (func.count(Obs.datum)/float(days)).label('completeness')

    # arrange data depending on upper or lower percentile
    order_dict = {'lower': Obs.datum.asc(),
                  'upper': Obs.datum.desc()}

    if method not in order_dict.keys():
        raise ValueError('Please enter a valid method type. Must be \'lower\' or \'upper\', ')

    order = order_dict[method]

    # percentile calculation
    percentile = (func.percentile_cont(quantile)
                      .within_group(order)
                      .label("air_temperature"))

    # This query returns rows containing the station_id and year
    # where there is a fully complete record over the month of interest,
    # as specified by start_time.month. Completeness is defined
    # in calculation.

    cte_full_cvg = (
        session.query(
            History.station_id.label('station_id'),
            func.extract('year', Obs.time).label('year')
        )
        .join(Variable, Obs.vars_id == Variable.id)
        .join(History, Obs.history_id == History.id)
        .filter(and_(Obs.time >= start_time, Obs.time < end_time))
        .filter(func.extract('month', Obs.time) == start_time.month)
        .filter(Variable.name == net_var_name)
        .filter(and_(Variable.standard_name == standard_name,
            Variable.cell_method == cell_method))
        .having(completeness == 1.0)
        .group_by(
            History.station_id.label('station_id'),
                func.extract('year', Obs.time).label('year')
        )
        .cte('complete')
    )

    # This query returns rows containing id's of eligible stations
    # (staisfying 8-year rule) with  with min_years or more fully complete
    # station records
    cte_yr_cvg = (
        session.query(
            cte_full_cvg.c.station_id
        )
        .having(func.count(cte_full_cvg.c.station_id) >= min_years)
        .group_by(cte_full_cvg.c.station_id)
        .cte('year')
    )

    # This query uses eligible station years from cte_full_cvg,
    # and eligible stations from cte_yr_cvg, and pools the observations
    # meeting both those criteria to calculate a percentile
    query = (
        session.query(
            percentile.label('quantile'),
            cte_yr_cvg.c.station_id,
            History.lat,
            History.lon
        )
        .select_from(Obs)
        .join(History, Obs.history_id == History.id)
        .join(cte_yr_cvg, History.station_id == cte_yr_cvg.c.station_id)
        .join(cte_full_cvg, History.station_id == cte_full_cvg.c.station_id)
        .filter(cte_full_cvg.c.year == func.extract('year', Obs.time))
        .group_by(
            cte_yr_cvg.c.station_id,
            History.lat,
            History.lon
        )
    )

    return query

def hdd(start_time, end_time, session):
    '''A query to get the heating degree days (hdd)
    'Degree Days Below 18C'. If start/end time
    range is longer than a year, then the average
    degree day across the annual range is used.
    Each comparison is made between 18C and a
    daily mean temperature.
    -----------------------------------------
    Returns:
        query (sqlalchemy query): sqlalchemy query object
        containing hdd values
    '''
    years = total_years(start_time, end_time)
    completeness = (count(Obs)/total_days(start_time, end_time)).label('completeness')
    # get heating degree days below 18 C, convert to celsius, take mean
    hdd = func.sum((180.0-Obs.datum)/years).label('hdd')
    query = (
             session.query(hdd,
                           min_time(Obs),
                           max_time(Obs),
                           History.lat,
                           History.lon,
                           History.station_id,
                           completeness
                           )
                    .select_from(Obs)
                    .join(Variable, Obs.vars_id == Variable.id)
                    .join(History, Obs.history_id == History.id)
                    .filter(and_(Obs.time >= start_time,
                                 Obs.time < end_time))
                    .filter(Variable.name == '3')
                    .filter(and_(Variable.standard_name == 'air_temperature',
                                 Variable.cell_method == 'time: mean'))
                    .filter(Obs.datum != 0.0) # bad obs are sometimes 0.0
                    .group_by(History.lat,
                              History.lon,
                              History.station_id)
             )

    return query
