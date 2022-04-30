from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext


conf = (
    SparkConf()
    .setMaster("spark://spark:7077")
    .setAppName("NetworkWordCount")
    .set("spark.dynamicAllocation.enabled", "false")
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
    .set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
    .set("spark.executor.memory", "512m")
    .set("spark.executor.instances", "2")
)

sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 1)

ssc.checkpoint("hdfs://hadoop:9000/checkpoint")

state_rdd = sc.emptyRDD()


def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


lines = ssc.socketTextStream("data_app", 9999)

words = lines.flatMap(lambda line: line.split(" ")).filter(lambda word: word != "")

pairs = words.map(lambda word: (word.lower(), 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)


windowWordCount = wordCounts.window(3 ,3)

wordsByWindowWithS = windowWordCount.filter(lambda x: x[0][0] == "s").reduce(lambda x, y: ("Words with S", x[1] + y[1]))
wordsByWindowWithP = windowWordCount.filter(lambda x: x[0][0] == "p").reduce(lambda x, y: ("Words with P", x[1] + y[1]))
wordsByWindowWithR = windowWordCount.filter(lambda x: x[0][0] == "r").reduce(lambda x, y: ("Words with R", x[1] + y[1]))

wordsByWindowWithSize6  = windowWordCount.filter(lambda x: len(x[0]) == 6 ).reduce(lambda x, y: ("Words with 6 characters", x[1] + y[1]))
wordsByWindowWithSize8  = windowWordCount.filter(lambda x: len(x[0]) == 8 ).reduce(lambda x, y: ("Words with 8 characters", x[1] + y[1]))
wordsByWindowWithSize11 = windowWordCount.filter(lambda x: len(x[0]) == 11 ).reduce(lambda x, y: ("Words with 11 characters", x[1] + y[1]))


running = wordCounts.updateStateByKey(updateFunc, initialRDD=state_rdd)

sorted = running.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

wordsLen = running.reduce(lambda x, y: ("Total number of words", x[1] + y[1]))


##sorted.pprint()
##wordsLen.pprint()
##wordsByWindowWithS.pprint()
##wordsByWindowWithP.pprint()
##wordsByWindowWithR.pprint()
wordsByWindowWithSize6.pprint() 
wordsByWindowWithSize8.pprint() 
wordsByWindowWithSize11.pprint() 

ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
