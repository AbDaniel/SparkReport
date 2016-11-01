from pyspark.sql.functions import window
from pyspark.sql.types import *

sparkSession = SparkSession\
        		.builder\
        		.appName("StructuredStreaming3")\
        		.getOrCreate()

staticDFPath = '/home/ubuntu/partb/input'

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
input = sc.textFile(staticDFPath)
users = input.map(lambda l: l.split("\n"))

inputSchema = StructType([ StructField("ID", StringType(), True) ])

staticDF = spark.createDataFrame(users, inputSchema)
staticDF.show()

path_to_directory='/home/ubuntu/partb/test/'

tweetSchema = StructType([ StructField("userA", StringType(), True), 
                                StructField("userB", StringType(), True), 
                                StructField("timestamp", TimestampType(), True), 
                                StructField("interaction", StringType(), True) ])
csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .schema(tweetSchema) \
    .csv(path_to_directory)


result = csvDF.join(staticDF, staticDF.ID == csvDF.userA, "inner")
new_result = result['ID', 'interaction', 'timestamp']

#count = staticDF.rdd.map(lambda x: csvDF.where(("userA = {0}").format(x.ID)))

counts = new_result.groupBy(new_result.ID).count()

query = counts\
        .writeStream\
        .trigger(processingTime='5 seconds')\
        .format('console')\
        .outputMode('complete')\
        .start()

