import numpy as np
from lmoments3 import distr
import pandas as pd
from pandas.api.types import is_numeric_dtype

class Gumbel:
    """This class is meant to do all the fitting
    of Gumbel distributions to extract design values.
    
    Attributes:
        return_period (int): return period in years
        min_fit (int): minimum number of years to consider
            fitting Gumbel.
    """

    def __init__(self, return_period, min_fit=10):

        if isinstance(return_period, int) is False:
            raise TypeError("return_period must be an integer.")

        if return_period < 1:
            raise ValueError("return_period must be greater than 1 year.")
            
        if isinstance(min_fit, int) is False:
            raise TypeError("min_fit must be an integer.")

        if min_fit < 2:
            raise ValueError("min_fit must be at least 2 to properly estimate parameters.")
        
        self.return_period = return_period
        self.min_fit = min_fit

    def fit(self, x):
        """Function to get L-moments to estimate the parameters
        of a gumbel distribution. This method uses the 
        right-skewed Gumbel distribution. Method mirrors 
        Hosking 1990.
        -----------------------------------
        Args:
            x (pandas Series): Numerical series of annual extremes
                grouped by station.
        Returns:
            (xi, alpha) (tuple): estimated parameters of gumbel
                distribution if N_min criteria met
            NaN (numpy NaN object): if N_min criteria is not met 
        """
        
        # number of years available from
        # at a station
        N = x.shape[0]

        # euler-mascheroni constant
        euler = np.euler_gamma

        if N >= self.min_fit:
            # create fitted gumbel object to
            # distribution of extreme values
            paras = distr.gum.lmom_fit(x)

            # extract L-moments
            lmoments = distr.gum.lmom(nmom=2, **paras)

            # calculate estimators of gumbel
            # parameters
            alpha = lmoments[1]/np.log(2)
            xi = lmoments[0] - euler*alpha 

            return xi, alpha
        else:
            return np.nan, np.nan

    def get_design_value(self, xi, alpha):
        """Get the design value from explicit expression
        derived from a right-skewed Gumbel distribution.
        See methods.pdf for more details. 
        -----------------------------------
        Args:
            xi (float): Estimated value of the location parameter
                (first l-moment)
            alpha (float): Estimated value of the scale parameter
                (second l-moment).
        Returns:
            design_val (float): design value using Gumbel parameters
        """
        if alpha <= 0.0:
            raise ValueError("alpha must be greater than 0.")

        # return frequency, i.e. inverse of return period 
        f_r = 1.0/self.return_period

        # simplify long expression
        simp = (1.0-f_r) + np.exp(-np.exp((xi/alpha)))

        # final expression for design value
        design_val = xi - alpha*np.log(-np.log(simp))

        return design_val

    def get_fit_transform(self, x):
        """Extract the design value.
        -----------------------------
        Args:
            x (pandas Series): Numerical series of annual extremes
                grouped by station.

        Returns:
            design value (float): the design value
                as extracted by get_design_value at
                the given return period.
        """
        xi, alpha = self.fit(x)
        return self.get_design_value(xi, alpha)

    def fit_transform(self, df, station_variable = 'station_id', variable='rainfall_rate'):
        """Creates a copy of input DataFrame with 
        extra column containing derived design values.
        ------------------------------------------
        Args:
            df (pandas DataFrame): Contains all observations
                of variable of interest
            station_variable (Str): Key for the unique 
                identifier for a station in the pandas 
                DataFrame. Default key is 'station_id'
            variable (Str): Key for the variable of interest.
                Default key is 'rainfall_rate'
        
        Returns:
            df_new (pandas DataFrame): Same as input dataframe
                but with added column containing the design values
                with suffix "_dval"
        """
        if isinstance(df, pd.DataFrame) == False:
            raise TypeError("df must be pandas DataFrame.")

        # Group stations and fits each station group independently.
        # Assigns value of get_fit_transform() to each row
        # of DataFrame for its related station group.
        df_new = df.join((df.groupby(station_variable)[variable]
                            .apply(self.get_fit_transform)), 
                         on=station_variable,
                         rsuffix='_dval')
        return df_new