# Work Sample Summary
Track 2 Data Engineering was chosen for this work sample. This problem was divided into three components: Cleanup, Label, Analysis. The submission is a single file, sparkws.py that can be run through spark-submit to produce the following solutions.

## A. Cleanup 
The goal for this component was to remove requests with identical geoinfo (Latitude, Longitude) and Timestamps, because they are considered suspicious. Utilizing `.dropDuplicates()` on the loaded sample data, a dataframe (table) will be created that only includes rows with unique geoinfo and timestamps. 

## B. Labels
The goal here was to assign each request (row of the dataframe) with the ID of the closest POI, from a separate info dataframe. In order to accomplish this, a function that could calculate the distance between two points, given their latitude and longitude had to be created. This function was used to calculate the distance between each request and POI, and added as a column to the dataframe. Using the functions `.groupBY()` and `.min()`, each request was assigned a POI based on  whichever POI it had the shortest distance with. 

## C. Analysis
This component was broken down into two parts.

### Part 1:
The desired result was to calculate the average and standard deviation for each POI listed. By utilizing the output from **B. Labels** along with pre-programmed functions for calculating mean and standard deviation, a table is created listing each POI and its statistics. 

### Part 2: 
In this part we use the output from **B. Labels** again to create a circle around the points depending on which POI is nearest, with the POI's co-ordinates as the circle centroid. We also determine each POI's circle's radius, and density (requests/area). The radius is initially determined as being the distance between the POI and the request it is farthest from (within the requests specified with the label of the POI). Utilizing the radius and the equation for the area of circle,  the bounding area for each POI was calculated, and used to determine density. The total number of requests in each POI/POI area gives us density. 