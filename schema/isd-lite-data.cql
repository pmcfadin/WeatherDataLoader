/*
Schema for storing raw ISD-lite hourly weather data.
More can be read about these weather sets here: http://www.ncdc.noaa.gov/oa/climate/isd/
*/

DROP KEYSPACE IF EXISTS isd_weather_data;
CREATE KEYSPACE isd_weather_data WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use isd_weather_data;

/*
Weather stations that correspond with collected raw weather data.
You can load this data by using the stations.csv file and the following copy command:

COPY weather_station (id, name, country_code, state_code, call_sign, lat, long, elevation) FROM 'stations.csv';

*/

CREATE TABLE weather_station (
    id text PRIMARY KEY,  // Composite of Air Force Datsav3 station number and NCDC WBAN number
    name text,            // Name of reporting station
    country_code text,    // 2 letter ISO Country ID
    state_code text,           // 2 letter state code for US stations
    call_sign text,       // International station call sign
    lat float,            // Latitude in decimal degrees
    long float,           // Longitude in decimal degrees
    elevation float       // Elevation in meters
);

/*
Raw weather readings from a single station, hourly.

You can try this COPY command but it's a lot of data to try to load. You'll be better off with some other loading program.
COPY raw_weather_data (weather_station, year, month, day, hour, temperature, dewpoint, pressure, wind_direction, wind_speed, sky_condition, one_hour_precip, six_hour_precip)
FROM '2005.csv';

*/

CREATE TABLE raw_weather_data (
   wsid text,               // Composite of Air Force Datsav3 station number and NCDC WBAN number
   year int,                // Year collected
   month int,               // Month collected
   day int,                 // Day collected
   hour int,                // Hour collected
   temperature double,       // Air temperature (degrees Celsius)
   dewpoint double,          // Dew point temperature (degrees Celsius)
   pressure double,          // Sea level pressure (hectopascals)
   wind_direction int,      // Wind direction in degrees. 0-359
   wind_speed double,        // Wind speed (meters per second)
   sky_condition int,       // Total cloud cover (coded, see format documentation)
   sky_condition_text text, // Non-coded sky conditions
   one_hour_precip double,   // One-hour accumulated liquid precipitation (millimeters)
   six_hour_precip double,   // Six-hour accumulated liquid precipitation (millimeters)
   PRIMARY KEY ((wsid), year, month, day, hour)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);


/*
Quick access lookup table for sky_condition. Useful for potential analytics.

Just in case you want to know what an okta is, here is the wikipedia page:
http://en.wikipedia.org/wiki/Okta
*/

CREATE TABLE sky_condition_lookup (
    code int PRIMARY KEY,
    condition text
);

INSERT INTO  sky_condition_lookup (code, condition) VALUES (0, 'None, SKC or CLR');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (1, 'One okta - 1/10 or less but not zero');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (2, 'Two oktas - 2/10 - 3/10, or FEW');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (3, 'Three oktas - 4/10');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (4, 'Four oktas - 5/10, or SCT');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (5, 'Five oktas - 6/10');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (6, 'Six oktas - 7/10 - 8/10');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (7, 'Seven oktas - 9/10 or more but not 10/10, or BKN');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (8, 'Eight oktas - 10/10, or OVC');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (9, 'Sky obscured, or cloud amount cannot be estimated');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (10, 'Partial obscuration 11: Thin scattered');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (12, 'Scattered');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (13, 'Dark scattered');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (14, 'Thin broken 15: Broken');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (16, 'Dark broken 17: Thin overcast 18: Overcast');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (19, 'Dark overcast');