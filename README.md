# viirs_ba

## Python
The Python script is “VIIRS_threshold_reflCor_Bulk.py” which drives all processes. Thresholds and other parameters are loaded using an initialization file (*.ini) that is provided as an argument at script execution (e.g., c:\VIIRS_threshold_reflCor_Bulk.py VIIRS_threshold_bulk.ini).

## Postgresql/PostGIS
A PostGIS database stores the point data generated from the VIIRS imagery thresholding and active fire detections. This database also groups point data into fire_events representative of a single fire.

### Terminology and Syntax
**Table syntax**: throughout this document, SQL style syntax is used to refer to tables and fields. For example “fire_events.collection_id” refers to the collection_id field in the fire_events table.

**Fire_event**: an entry in the fire_events table that consists of active fire detections and thresholded burned area that meets the spatial and temporal criteria.

**Fire_collection**: an entry in the fire_collections table that groups fire_events (individual pixels) into individual fires through the use of the foreign key found in fire_collections (join on fire_events.collection_id = fire_collections.fid). 

## Process

All processing is initiated and controlled by the python script VIIRS_threshold_reflCor_Bulk.py. On execution, the script:
*	reads the initialization file
*	loops through available imagery
*	thresholds each image
*	pushes thresholded and active fire pixel coordinates to database
*	initiates database functions, groups detected pixels into collections of contiguous fires
*	exports shape file of thresholded and active fire detections
