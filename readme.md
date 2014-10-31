## Collection of utilities to work with NOAA weather data

Data can be found here
ftp://ftp3.ncdc.noaa.gov/pub/data/noaa/isd-lite/

### NOAAFileProcessor
Files from the ftp site will come as one file per weather station, per year. This processor will:

- Convert position dependant to csv
- Apply the proper scaling to each field. Decimals are not in pos dependant files, so they must be divided by a factor of 10.
- Combine all station files for a year into a single file
- GZip the final file

Directory structure of downloaded files should be `<data_directory>/<year>`

### Usage

`NOAAFileProcessor <data_directory> <output_directory>` 

