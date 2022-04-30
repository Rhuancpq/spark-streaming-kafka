from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


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

sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 1)

ssc.checkpoint("hdfs://hadoop:9000/checkpoint")

state_rdd = sc.emptyRDD()


def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


lines = KafkaUtils.createStream(
    ssc, "zookeeper:2181", "spark-streaming-consumer", {"data-topic": 1}
)

# FIXME interface changes
words = lines.flatMap(lambda line: line.split(" ")).filter(lambda word: word != "")

pairs = words.map(lambda word: (word.lower(), 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

running = wordCounts.updateStateByKey(updateFunc, initialRDD=state_rdd)

sorted = running.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

sorted.pprint()

ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
