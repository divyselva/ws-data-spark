
# # Preparation/ Set Up
 
# - **Import necessary libraries**
 
# - **Create Spark Session**

# - **Read CSVs into dataframes**
#     - Modify headers for easier use

from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


sc = SparkContext("local", "EQ WORKS PROBLEM SET")
spark = SparkSession(sc)

df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('/tmp/data/DataSample.csv')

df.printSchema()

poi = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('/tmp/data/POIList.csv')

poi.printSchema()

df = df.withColumnRenamed(' TimeSt', 'TimeSt')

poi = poi.withColumnRenamed(' Latitude','PLat').withColumnRenamed('Longitude','PLong')


# **Create haversine function to calculate distance between points**
# - Use for when determining minimum distance and radius

def dist(lat1, long1, lat2, long2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lat1, long1, lat2, long2 = map(f.toRadians, [lat1, long1, lat2, long2])
    # haversine formula 
    dlon = long2 - long1 
    dlat = lat2 - lat1 
    a = f.sin(dlat/2)**2 + f.cos(lat1) * f.cos(lat2) * f.sin(dlon/2)**2
    c = 2 * f.asin(f.sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return f.round(km, 3)

# # A. Clean-up
 
# **Remove duplicates from DataSample**

# 4052 rows removed

df = df.dropDuplicates(['TimeSt','Latitude','Longitude'])

# Also removed duplicate POI (Kept POI 1, dropped POI 2 )
poi = poi.dropDuplicates(['PLat','PLong'])


# # B. Label
# **Combined both dataframes to identify POI with minimum distance, utilizes haversine function**

combine = df.crossJoin(poi).withColumn("distance", dist('Latitude','Longitude','PLat','PLong'))

minlabel = combine.groupBy('_ID').min('distance').withColumnRenamed('min(distance)','distance').join(combine,['_ID','distance'])

minlabel = minlabel.dropDuplicates(['_ID','distance'])

# **Each request is assigned to the closes POI**
minlabel.show(5)

# # C. Analysis P1

# **Calculate average and std. deviation of distance for each POI**

analysis1 = minlabel.groupBy('POIID').agg({'distance':'mean'}).join(minlabel.groupBy('POIID').agg({'distance':'stddev'}),'POIID')

analysis1.show()

# # P2
 
# **Create circle with POI as central point, inclusive to all of its requests**
# **Calculate each POI circle's area and density**
 
# - The radius is determined as being the distance to the farthest request included for the POI

analysis2 = minlabel.groupBy('POIID').agg({'distance':'max'}).join(minlabel.groupBy('POIID').count(), 'POIID').withColumnRenamed('max(distance)','radius km')

analysis2 = analysis2.withColumn('Density req/km2', f.round(f.col('count') / (3.1412 * f.col('radius km')* f.col('radius km'))))

analysis2.show()