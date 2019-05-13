from pycds import Obs, History, Variable, Station
from sqlalchemy.sql import func
from sqlalchemy.orm import aliased
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
                           Station.native_id,
                           completeness)
                    .select_from(Obs)
                    .join(Variable, Obs.vars_id == Variable.id)
                    .join(History, Obs.history_id == History.id)
                    .join(Station, History.station_id == Station.id)
                    .filter(and_(Obs.time >= start_time,
                                 Obs.time < end_time))
                    .filter(Variable.standard_name == 'rainfall_rate')
                    .filter(Variable.name == '127')
                    .group_by(func.extract("year", Obs.time),
                              History.lat,
                              History.lon,
                              History.station_id,
                              Station.native_id,
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

    date = min_time(Obs)
    max_val = func.max(Obs.datum)

    q = (
         session.query(max_val.label("rate"),
                           date)
                    .filter(Variable.name == '127')
                    .group_by(func.extract('day', Obs.time))
        ).subquery()

    q_rain = (
             session.query(max_val.label("total_rain"),
                            date)
                    .filter(and_(Obs.time >= start_time,
                                 Obs.time < end_time))
                    .filter(or_(Variable.name == '48',
                                Variable.name == '10'))
                    .group_by(func.extract('day', Obs.time))
        ).subquery()

    qq = (session.query(q.columns.rate, q_rain.columns.total_rain,
                       q.columns.min_time, q_rain.columns.min_time, History.lat,
                       History.lon,
                       History.station_id)
                        .select_from(Obs))

    new_q = (qq.join(q_rain, q.columns.min_time==q_rain.columns.min_time)
             .filter(q.columns.min_time==q_rain.columns.min_time)
             .filter(q_rain.columns.total_rain > 1.0)
             .limit(1000))

    '''( session.query(s.columns.total_rain,
                            p.columns.rate,
                            s.columns.rain_time,
                            p.columns.rate_time,
                            History.lat,
                            History.lon,
                            History.station_id)
                    .join(p, s.columns.rain_time==p.columns.rate_time)
                    .join(History, Obs.history_id == History.id)
                    .join(Variable, Obs.vars_id == Variable.id).limit(10000)
            )'''

    return new_q

#join(Variable, Obs.vars_id == Variable.id).join(History, Obs.history_id == History.id).limit(10000)
#outerjoin(s, Obs.time==s.columns.obs_time).limit(10000)
#return newq.join(s, Obs.time==s.columns.obs_time).limit(10000)

#    return query