from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql import functions as F

if __name__ == "__main__":
    if len(sys.argv) != 5 and len(sys.argv) != 4:
        msg = ("Usage: StructuredStreaming1.py <path_to_directory>")
        print(msg, file=sys.stderr)
        exit(-1)

    path_to_directory = sys.argv[1]

    spark = SparkSession\
        .builder\
        .appName("StructuredStreaming1")\
        .getOrCreate()

    path_to_directory='/home/ubuntu/partb/test/'
    # Create DataFrame representing the stream of input lines from connection to host:port
    tweetSchema = StructType([ StructField("userA", StringType(), True), 
                                StructField("userB", StringType(), True), 
                                StructField("timestamp", TimestampType(), True), 
                                StructField("interaction", StringType(), True) ])
    csvDF = spark \
        .readStream \
        .option("sep", ",") \
        .schema(tweetSchema) \
        .csv(path_to_directory)    # Equivalent to format("csv").load("/path/to/directory")


    csvDF.isStreaming
    csvDF.schema==tweetSchema
    csvDF.printSchema()
    #query = csvDF.groupBy('interaction').count
    #x = csvDF.writeStream.format('console').outputMode('append').queryName('query').start()


    #qx = csvDF.select(csvDF.userB, F.when(csvDF.interaction == 'MT', csvDF.userB))

    from pyspark.sql import functions as F
    #q = csvDF.select(F.when(csvDF.interaction == 'MT', csvDF.userB))
    #q2 = q.select(F.when(q['CASE WHEN (interaction = MT) THEN userB END'] != 'null'))

    #q3 = csvDF.select(window(csvDF.timestamp, '10 seconds'), csvDF.userB, F.when(csvDF.interaction == 'MT', csvDF.userB))

    q3 = csvDF.select(window(csvDF.timestamp, '10 seconds'), "userB").where("interaction = 'MT'")

    query = q3\
        .writeStream\
        .trigger(processingTime='10 seconds')\
        .format('console')\
        .start()

    query.awaitTermination()
