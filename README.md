# api_integration_etl_pipeline

This pipeline extracts weather data from an online service via api integration.

The first step is to get a random city name from a pre-populated (cityLust.yaml) file then it extracts the iso3 code from a populated db table within the weatherData.db. The iso3 code and city name are passed as parameters to a geocode api to get the corresponding coordinates of the city.

The lon and lat (coordinates) are then used as parameters to extract the weather information from the weather api and fed into the db for further analysis / report / dashboard.

The next step is to build in error handling to take care of situations where the city name search returns a null value. In this case, the process starts afresh to pick another random city and query the db till it gets a match in teh db.
