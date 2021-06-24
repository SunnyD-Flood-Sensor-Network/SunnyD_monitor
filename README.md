# SunnyD_monitor
Code to process real-time data using R with Docker.

Every **6** minutes, this loop:
- filters a postgres database for new data ("processed == F")
- downloads atmospheric pressure data from nearby NOAA station that bounds the new data
- converts pressure data into water level and compensates for atmospheric pressure
- flags suspicious data points using column "qa_qc_flag" (large variations in pressure values)
- writes processed data to new database & updates raw data database to note that rows have been processed ("processed = T"). If rows of data already exist in the processed data table, they are updated - this would occur if data are reanalyzed
