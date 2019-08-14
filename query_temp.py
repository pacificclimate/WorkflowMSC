from sqlalchemy.sql import func
from sqlalchemy import and_, or_, case

from pycds import Obs, History, Variable

from query_helpers import total_days, total_years, count, min_time, max_time, days_in_month, hours_in_month

def query_design_temp_percentile(start_time, end_time, session, quantile=0.01):
    """A query to get the percentile of minimum daily air_temperatures
    at a station in a given time frame. Daily minimum air
    temperatures are used.
    -----------------------------------------
    Args:
        session (sqlalchemy Session): session constructed using
            connection string and engine
        percentile (float): desire percentile in fraction
        month (datetime): desired month in which to calculate
            the desired percentile, default is 1, or January.
    Returns:
        query (sqlalchemy query): sqlalchemy query object
            for design values
    """

    percentile = (func.percentile_cont(quantile)
                 .within_group(Obs.datum.asc())
                 .label("air_temperature"))
    days = days_in_month(start_time, end_time)
    completeness = (count(Obs)/days).label("completeness")

    month = start_time.month
    query = (
             session.query(percentile,
                           min_time(Obs),
                           max_time(Obs),
                           History.lat,
                           History.lon,
                           History.station_id,
                           Variable.name,
                           Variable.standard_name,
                           completeness)
                    .select_from(Obs)
                    .join(Variable, Obs.vars_id == Variable.id)
                    .join(History, Obs.history_id == History.id)
                    .filter(and_(Obs.time >= start_time,
                                 Obs.time < end_time))
                    .filter(func.extract("month", Obs.time) == month)
                    #.filter(Variable.name == '2')
                    .filter(Variable.standard_name == 'air_temperature')
                    .filter(Variable.standard_name == 'soil_temperature')
                    .filter(Variable.standard_name == 'surface_air_pressure')
                    .filter(Variable.standard_name == 'wet_bulb_temperature')
                    .filter(Variable.standard_name == 'relative_humidity')
                    #.filter(Obs.datum != 0.0) # bad obs are sometimes 0.0
                    .group_by(
                            History.lat,
                            History.lon,
                            History.station_id,
                            Variable.name,
                            Variable.standard_name
                    )
             )

    return query

def query_all_temp(start_time, end_time, session):
    """A query to get the percentile of minimum daily air_temperatures
    at a station in a given time frame. Daily minimum air
    temperatures are used.
    -----------------------------------------
    Args:
        session (sqlalchemy Session): session constructed using
            connection string and engine
        percentile (float): desire percentile in fraction
        month (datetime): desired month in which to calculate
            the desired percentile, default is 1, or January.
    Returns:
        query (sqlalchemy query): sqlalchemy query object
            for design values
    """

    days = days_in_month(start_time, end_time)
    percentile = (func.percentile_cont(.10)
                 .within_group(Obs.datum.asc())
                 .label("air_temperature"))
    completeness = (count(Obs)/days).label("completeness")

    month = start_time.month
    query = (
             session.query( percentile,
                            completeness,
                            func.min(Obs.datum),
                            func.max(Obs.datum),
                            func.min(Obs.time),
                            func.max(Obs.time),
                            History.lat,
                            History.lon,
                            History.station_id,
                            Variable.name,
                        )
                    .select_from(Obs)
                    .join(Variable, Obs.vars_id == Variable.id)
                    .join(History, Obs.history_id == History.id)
                    .filter(and_(Obs.time >= start_time,
                                 Obs.time < end_time))
                    .filter(func.extract("month", Obs.time) == month)
                    .filter((Variable.name == '1') |
                            (Variable.name == '2') |
                            (Variable.name == '3')
                            )
                    .filter((Variable.standard_name == 'air_temperature'))
                    #.filter(Obs.datum != 0.0) # bad obs are sometimes 0.0
                    .group_by(
                            History.lat,
                            History.lon,
                            History.station_id,
                            Variable.name,
                            Variable.standard_name,)
             )

    return query


def query_design_temp_dry(start_time, end_time, session, quantile=0.025):
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

    percentile = (func.percentile_cont(quantile)
                 .within_group(Obs.datum.desc())
                 .label("air_temperature"))
    days = days_in_month(start_time, end_time)
    completeness = (count(Obs)/days).label("completeness")

    month = start_time.month
    # construct query table
    query = (
             session.query(percentile,
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

def query_design_temp_wet(start_time, end_time, session, quantile=0.025):
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


    percentile = (func.percentile_cont(quantile)
                 .within_group(Obs.datum.desc())
                 .label("air_temperature"))

    days = days_in_month(start_time, end_time)
    hours = hours_in_month(start_time, end_time)

    daily_complete = (count(Obs)/days)
    hourly_complete = (count(Obs)/hours)

    month = start_time.month

    # NOTE: DBMSC has mix-matched frequency of sampling for wet bulb measurements.
    # Some are daily and some are hourly. If there are more observations greater
    # than the days in 1 month then it assumes hourly. This does not account for
    # undersampled hourly measurements, and assumes that if a station is sampled
    # hourly, it has at least more obs than days in 1 month. Thus, extremely bad
    # hourly data may be in this query, however, if any, it is likely sparse.

    # create a condition that separates daily and hourly data and guesses freqw
    completeness = (
            case([(func.count(Obs.datum) <= days,
                   daily_complete)],
                   else_= hourly_complete).label('completeness')
            )

    query = (
             session.query(percentile,
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
                    .filter(func.extract("month", Obs.time) == month)
                    .filter(Variable.name == '79')
                    .filter(Variable.standard_name == 'wet_bulb_temperature')
                    .filter(Obs.datum != 0.0) # bad obs are sometimes 0.0
                    .group_by(History.lat,
                              History.lon,
                              History.station_id)
             )

    return query

def hdd(start_time, end_time, session):
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
    years = total_years(start_time, end_time)
    completeness = count(Obs)/total_days(start_time, end_time)
    # get heating degree days below 18 C, convert to celsius, take mean
    hdd = func.sum((180.0-Obs.datum)/years).label("hdd")
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

def simple_air_temp(session, station_id):

    query = (
             session.query(Obs.datum,
                           History.lat,
                           History.lon,
                           History.station_id,
                    )
                    .select_from(Obs)
                    .join(Variable, Obs.vars_id == Variable.id)
                    .join(History, Obs.history_id == History.id)
                    .filter(History.station_id == station_id)
                    .filter(Variable.standard_name == 'air_temperature')
    )

    return query
