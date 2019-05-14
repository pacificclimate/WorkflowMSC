from sqlalchemy.sql import func
from sqlalchemy import and_, or_, case, distinct

from calendar import monthrange
from pycds import Obs, History, Variable

from query_helpers import total_days, total_years, count, min_time, max_time, days_in_month, hours_in_month


def full_cvg(session,
             start_time,
             end_time,
             completeness,
             var_name,
             cell_method,
             standard_name):

    sub_full_cvg = (session.query(completeness,
                                  History.station_id.label('station_id'),
                                  func.extract('year', Obs.time).label('year'))
                           .join(Variable, Obs.vars_id == Variable.id)
                           .join(History, Obs.history_id == History.id)
                           .filter(and_(Obs.time >= start_time,
                                        Obs.time < end_time))
                           .filter(func.extract('month', Obs.time) == start_time.month)
                           .filter(Variable.name == var_name)
                           .filter(and_(Variable.standard_name == standard_name,
                                        Variable.cell_method == cell_method))
                           .having(completeness == 1.0)
                           .group_by(func.extract('year', Obs.time),
                                     History.station_id)).subquery()

    return sub_full_cvg

def yr_cvg(session,
           start_time,
           end_time,
           completeness,
           var_name,
           cell_method,
           standard_name,
           min_years):

    sub_full_cvg = full_cvg(session, start_time, end_time, completeness, var_name, cell_method, standard_name)

    sub_yr_cvg = (session.query(sub_full_cvg.columns.station_id.label('station_id'),
                                func.count(sub_full_cvg.columns.station_id).label('count'))
                         .having(func.count(sub_full_cvg.columns.station_id) >= min_years)
                         .group_by(sub_full_cvg.columns.station_id)).subquery()

    return sub_yr_cvg

def hist(session):

    sub_hist = (session.query(History.lat, History.lon, History.station_id)
                       .group_by(History.lat, History.lon, History.station_id)
                       .subquery())

    return sub_hist

def temp_quantile(session,
                  start_time,
                  end_time,
                  method='lower',
                  cell_method='time: minimum',
                  bulb='dry',
                  var_name='2',
                  standard_name='air_temperature',
                  quantile=0.01,
                  min_years=8):

    '''A query to get the percentile of minimum daily air_temperatures
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
            for design value
    '''

    # get the number of days in the month of the start_time
    days = monthrange(start_time.year, start_time.month)[1]

    # calculate completeness between expected observations and actual obs
    completeness = (func.count(Obs.datum)/float(days)).label('completeness')

    if method is 'lower':
        order = Obs.datum.asc()

    elif method is 'upper':
        order = Obs.datum.desc()

    else:
        raise ValueError('Please enter a valid method type. Must be \'lower\' or \'upper\', ')

    if bulb is 'wet':
        completeness = (
                        case([(func.count(Obs.datum) <= days,
                               daily_complete)],
                        else_= hourly_complete).label('completeness')
                        )

    elif bulb is not 'wet' and bulb is not 'dry':
        raise ValueError('Please enter a valid bulb type as \'wet\' or \'dry\'')

    percentile = (func.percentile_cont(quantile)
                      .within_group(order)
                      .label("air_temperature"))

    # This has the eligible years
    sub_full_cvg = full_cvg(session,
                            start_time,
                            end_time,
                            completeness,
                            var_name,
                            cell_method,
                            standard_name)

    # This has the eligible stations
    sub_yr_cvg = yr_cvg(session,
                        start_time,
                        end_time,
                        completeness,
                        var_name,
                        cell_method,
                        standard_name,
                        min_years)

    sub_hist = hist(session)

    # Take eligible stations from yr_cvg, and eligible years from full_cvg to get the correct obs
    query = (session.query(
                            percentile.label('quantile'),
                            sub_hist.columns.station_id,
                            sub_hist.columns.lat,
                            sub_hist.columns.lon
                           )
                    .join(History, Obs.history_id == History.id)
                    .join(sub_full_cvg, History.station_id == sub_full_cvg.columns.station_id)
                    .join(sub_yr_cvg,  History.station_id == sub_yr_cvg.columns.station_id)
                    .join(sub_hist, History.station_id == sub_hist.columns.station_id)
                    .filter(sub_full_cvg.columns.year == func.extract('year', Obs.time))
                    .group_by(sub_hist.columns.station_id,
                              sub_hist.columns.lat,
                              sub_hist.columns.lon)
             )

    return query

def query_design_temp_wet(start_time, end_time, session, quantile=0.025):
    '''A query to get the percentile of maximum daily
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
    '''

    # get the number of days in the month of the start_time
    days = monthrange(start_time.year, start_time.month)[1]

    month = start_time.month

    # completeness for daily freq
    daily_complete = (count(Obs)/days)
    # compeleteness for hourly freq
    hourly_complete = (count(Obs)/hours)

    # calculate completeness between expected observations and actual obs
    completeness = (
                    case([(func.count(Obs.datum) <= days,
                           daily_complete)],
                        else_= hourly_complete).label('completeness')
                    )


    # This has the eligible years
    full_cvg = (session.query(completeness,
                              History.station_id.label('station_id'),
                              func.extract('year', Obs.time).label('year')
                              )
                       .join(Variable, Obs.vars_id == Variable.id)
                       .join(History, Obs.history_id == History.id)
                       .filter(and_(Obs.time >= start_time,
                                    Obs.time < end_time))
                       .filter(func.extract('month', Obs.time) == month)
                       .filter(Variable.name == var_name)
                       .filter(and_(Variable.standard_name == 'air_temperature',
                                    Variable.cell_method == min_max))
                       .having(completeness == 1.0)
                       .group_by(func.extract('year', Obs.time),
                                 History.station_id)
                )

    sub_full_cvg = full_cvg.subquery()

    # This has the eligible stations
    yr_cvg = (session.query(
                            sub_full_cvg.columns.station_id.label('station_id'),
                            func.count(sub_full_cvg.columns.station_id).label('count')
                            )
                     .having(func.count(sub_full_cvg.columns.station_id) >= 8)
                     .group_by(sub_full_cvg.columns.station_id)
              )

    sub_yr_cvg = yr_cvg.subquery()

    sub_hist = (
                session.query(History.lat, History.lon, History.station_id)
                       .group_by(History.lat, History.lon, History.station_id)
                       .subquery()
                )

    # Take eligible stations from yr_cvg, and eligible years from full_cvg to get the correct obs
    query = (session.query(
                            percentile.label('quantile'),
                            sub_hist.columns.station_id,
                            sub_hist.columns.lat,
                            sub_hist.columns.lon
                           )
                    .join(History, Obs.history_id == History.id)
                    .join(sub_full_cvg, History.station_id == sub_full_cvg.columns.station_id)
                    .join(sub_yr_cvg,  History.station_id == sub_yr_cvg.columns.station_id)
                    .join(sub_hist, History.station_id == sub_hist.columns.station_id)
                    .filter(sub_full_cvg.columns.year == func.extract('year', Obs.time))
                    .group_by(sub_hist.columns.station_id,
                              sub_hist.columns.lat,
                              sub_hist.columns.lon)
             )


    percentile = (func.percentile_cont(quantile)
                 .within_group(Obs.datum.desc())
                 .label('air_temperature'))

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

    #completeness = hourly_complete.label('completeness')

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
                    .filter(func.extract('month', Obs.time) == month)
                    .filter(Variable.name == '79')
                    .filter(Variable.standard_name == 'wet_bulb_temperature')
                    .group_by(History.lat,
                              History.lon,
                              History.station_id)
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
