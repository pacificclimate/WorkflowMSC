# QueryMSC
QueryMSC is a python tool to query design values from Meteorological Service of Canada (MSC) database hosted @pacificclimate. This project is currently in development 

## Methods
_Design values_ are physical and statistical derivations from samples of meteorological data that describe a given location's climatology and help inform the [_National Building Code of Canada_](https://www.nrc-cnrc.gc.ca/eng/publications/codes_centre/2015_national_building_code.html). Canada has a large suite of historical meteorological data that are used to derive design values.

Most design values in this project are derived based on a description found in [_National Building Code of Canada 2015_](https://www.nrc-cnrc.gc.ca/eng/publications/codes_centre/2015_national_building_code.html) _Volume 1 Appendix C_. 

### Gumbel Distribution

---

Some non-trivial statistical methods regarding the use of Gumbel extreme value distributions for rainfall amounts are described here. 



The Gumbel distribution, also known as the Generalized Extreme Value distribution Type-I, models maximum and minimum values of extreme values. Some design values, such as *15 Min Rain* and *One Day Rain 1/50* both require a fitting of the Gumbel distribution to annual maximum 15 minute rainfall and daily rainfall at a given station. In simpler terms, it provides a statistically robust and accurate way of determining the likelihood of extreme weather events to occur within a given time frame based on a historical record of a given weather station. 



The general form of the Cumulative Distribution Function (CDF) for the right-skewed Gumbel distribution is given by:



\\[ F(X) = e^{- e^{- (X-\mu)/\beta}} \\]



Where $X​$ is a random variable with a Gumbel distribution of N elements, $\mu​$ and $\beta​$ are the first two moments of the Gumbel distribution. The value of $\mu​$ and $\beta​$, in practice, are estimated by moments derived from $X​$ which will be the topic of this section. 



The CDF of a distribution gives the probability of an event occurring between two values spanned by the distribution. For the purposes of Extreme Value Analysis (EVA) in climatology, extreme weather events are characterized by their *return period* (usually in years), and the probability that an event would occur once within the return period.



Let $t_r$ be the return period in years, and then let $f_r$ be the return frequency, and be defined as $f_r  = \frac{1}{tr}$ with units of years$^{-1}$. It follows that the probability of an annual event occurring within $t_r$ years is simply $f_r$.



We can then express the probability, $P(X)$, of having an event occur within $t_r$ using the CDF of the Gumbel distribution, $F(X)$.  



$$P(0 \leq X \leq x_v) = F(x_v) - F(0) = f_r​$$



where $x_v$ is the magnitude of the extreme weather that occurs with probability $f_r$. Note that $F(0)$ is the least extreme case of a given weather event. 



To estimate $\mu$ and $\beta​$, *L-moments* are used following the methods published by [*Hosking, 1990*](https://www.jstor.org/stable/2345653#metadata_info_tab_contents). The main motivators for using *L-moments*, as opposed to more conventional estimators, such as the *Method of Moments* found in [*Newark et al. 1988*](https://www.nrcresearchpress.com/doi/pdf/10.1139/l89-052), is that *L-moments* are robust and resistant despite the nature of highly variable data, and very large outliers. Although *L-moments* are not completely resistant, they are more so than *mean* or *standard deviation*.



*L-moments* must be estimated from samples drawn from an unknown distribution, and in practice, this is done using U-statistics introduced by [*Hoeffding, 1948*](https://projecteuclid.org/download/pdf_1/euclid.aoms/1177730196). 



For a Gumbel distribution, only the first two *L-moments* need to be calculated. Let $N$ be the sample size, and $X_i$ be the ordered sample.



$$l_1 = n^{-1} \sum_i^N{X_i}$$

$$l_2 = \frac{1}{2}\left(N \atop 2\right)^{-1}{\sum\sum \atop i \gt j}{\left(X_{i:N} - X_{j:N}\right)}​$$



Then if $\hat{\xi}$ and $\hat{\alpha}$ estimate $\mu$ and $\beta$ respectively, then $\hat{\xi}$ and ​$\hat{\alpha}$ can be expressed by



$$\hat{\xi} = l_1 - \gamma \hat{\alpha}​$$

and,

$$\hat{\alpha} = \frac{l_2}{\log{2}} ​$$



Substituting  $\hat{\xi}$ and $\hat{\alpha}$ into $$P(0 \leq X \leq x_v)$$ gives us 



$$f_r = e^{-e^{-(x_v - \hat{\xi})/\beta}} - e^{-e^{\hat{\xi}/\beta}}$$



Solving for $x_v$, the magnitude of the extreme weather event with probability $f_r$ gives



$$x_v = \hat{\xi} - \hat{\alpha}\log{\left(-\log\left(f_r +  e^{-e^{\hat{\xi}/\beta}}\right)\right)}​$$



For a given station, each parameter in the above equation can be calculated from it's distribution of annual maximum values.  



Note that the requirement $N \geq 10$ is used in order to calculate the estimators better.