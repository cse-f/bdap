check zookeeper is running or not in 2181
kafka-server
create,list
producer
program
consumer


wget --no-check-certificate https://archive.apache.org/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz
tar -xzf kafka_2.10-0.8.2.2.tgz
sudo mv kafka_2.10-0.8.2.2 /usr/lib/kafka
cd /usr/lib/kafka

to run zookeepr start port at 2181
bin/zookeeper-server-start.sh config/zookeeper.properties
to know whether it is running or not
sudo netstat -tulnp | grep 2181

to run kafka port at 9092
bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic
to check all topoics
bin/kafka-topics.sh --list --zookeeper localhost:2181

producer
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

in home/cloudera
nano spark_socket_consumer.py

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName("SocketKafkaForwardConsumer").setMaster("local[2]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)  # 2-second batch interval

sc.setLogLevel("WARN")

lines = ssc.socketTextStream("127.0.0.1", 9999)

def process(rdd):
    count = rdd.count()
    if count > 0:
		print("Received %d records in this batch" % count)
		for i, record in enumerate(rdd.take(10), start=1):
			print("[{0}] {1}".format(i, record))
    else:
		print("No records in this batch")

lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()


ctrl+o enter ctrl+x

now find spark
it will be in usr/lib
cd /usr/lib
cd spark
ls bin ----you should able to see spark-submit

bin/spark-submit --master local[2] /home/cloudera/spark_socket_consumer.py

consumer
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test-topic --from-beginning
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test-topic --from-beginning | nc -lk 9999


reset
first listen(socket server) then start the consumer to send the data back
nc -lk 9999
to listen

nc localhost 9999
to send


to test first nc -lk 
then start socket
and enter anything in nc -lk terminal


