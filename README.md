# QueryMSC
QueryMSC is a python tool to query design values from Meteorological Service of Canada (MSC) database hosted @pacificclimate. This project is currently in development 

## Methods
_Design values_ are physical and statistical derivations from samples of meteorological data that describe a given location's climatology and help inform the [_National Building Code of Canada_](https://www.nrc-cnrc.gc.ca/eng/publications/codes_centre/2015_national_building_code.html). Canada has a large suite of historical meteorological data that are used to derive design values.

Most design values in this project are derived based on a description found in [_National Building Code of Canada 2015_](https://www.nrc-cnrc.gc.ca/eng/publications/codes_centre/2015_national_building_code.html) _Volume 1 Appendix C_. 

### Gumbel Distribution

---

Some non-trivial statistical methods regarding the use of Gumbel extreme value distributions for rainfall amounts are described [here](./methods.pdf). 
