
# Transit Passenger Surveys

Transit passenger data collection and analysis. 

## Workplan (Last updated: 01/12/2026)
- [x] Create placeholder installable `transit_passenger_surveys` Python package in repo
- [ ] Create Python `requests` scripts to replace R code in `requests` folder, this is just one-off scripts to respond to specific data requests -- *in progress*
- [ ] Standardized canonical onboard survey schema from `instrument-and-dictionary` -- *in progress*
- [ ] Migrate `make-uniform` code into `transit_passenger_surveys` package -- *or is it even needed if standardized schema is defined?*
- [ ] Migrate `summaries` code into `transit_passenger_surveys` package



## Legacy code
All of this has been moved into `archive` folder and will be maintained there for reference as they get gradually replaced by python code in the `transit_passenger_surveys` package.

### Survey data collection and processing

* [Instrument and Dictionary](https://github.com/BayAreaMetro/onboard-surveys/tree/master/instrument-and-dictionary): Survey questions and data collected. *[Last changed 2025]*

* [Make Uniform](https://github.com/BayAreaMetro/onboard-surveys/tree/master/make-uniform): Process raw data from different surveys and transit agencies into the uniform format. *[Last changed 2025]*

* [Mode Choice Targets](https://github.com/BayAreaMetro/onboard-surveys/tree/master/mode-choice-targets): Process OBS and generate inputs for mode choice targets. *[Last changed 2016]*


### Survey data analytics

* [Summaries](https://github.com/BayAreaMetro/onboard-surveys/tree/master/summaries): High-level statistical analysis on survey data in R and Tableau. *[Last changed 2025]*

* [Decomposition](https://github.com/BayAreaMetro/onboard-surveys/tree/master/decomposition): Route-level statistical analysis on survey data in R and Tableau. *[Last changed 2018]*

* [Requests](https://github.com/BayAreaMetro/onboard-surveys/tree/master/requests): Analyze survey data to respond data requests from stakeholders. *[Last changed 2025]*

### Exploratory data analytics

* [Multi-criteria Expansion](https://github.com/BayAreaMetro/onboard-surveys/tree/master/multi-criteria-expansion): Explore ways to ind an optimal set of expansion weights that best satisfy any number of criteria. *[Last changed 2015]*
* [On-off Explore](https://github.com/BayAreaMetro/onboard-surveys/tree/master/on-off-explore): Explore the value of performing an on/off pre-survey for small operators. *[Last changed 2015]*

* [Travel Model Priors](https://github.com/BayAreaMetro/onboard-surveys/tree/master/travel-model-priors): Process boarding and alighting data from SF Muni automated passenger counters, boarding and alighting flows from the SFCTA SF-CHAMP travel model, on-to-off surveys on SF Muni, etc. *[Last changed 2016]*