from calendar import monthrange

from pycds import Variable, Obs
from sqlalchemy.sql import func

def max_time(obs):
    return func.max(obs.time).label("max_time")

def min_time(obs):
    return func.min(obs.time).label("min_time")

def count(obs):
    return func.count(obs.datum).label("obs_count")

def total_days(start_time, end_time):
    return float((end_time - start_time).days)

def total_years(start_time, end_time):
    return float(end_time.year - start_time.year)

def days_in_month(start_time, end_time):

    year_range = range(start_time.year, end_time.year)

    if start_time.month == end_time.month:
        month = start_time.month
    else:
        raise ValueError("Starting and ending month are different. Must be same.")
    days = [monthrange(year, month)[1] for year in year_range]

    return float(sum(days))

def hours_in_month(start_time, end_time):
    return days_in_month(start_time, end_time)*24.0


def query_variables(session):
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

# create a condition that separates daily and hourly data and calculates
# the completeness based on total number of theoretical observations
#self.daily_complete = (self.count/self.total_days).label('completeness')
#self.hourly_complete = (self.count/self.total_hours).label('completeness')
#self.hourly_complete_yr = (self.count/((self.total_days/self.yr_interval)*24*4)).label('completeness')
#self.month_days_complete = (self.count/self.days_in_month).label('completeness')