download kafka before 4.0 version
config folder
change server properties --- logs directory
zoo keeper properties ---- data directory
start cmd in kafka folder

and run the following(to start the zookeeper server)
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
now start kafka server
.\bin\windows\kafka-server.start.bat .\config\server.properties

now we have two cmds running zookeeper and then kafka server
now we need to have producer and consumer
these two will be present in bin/windows only in the name of console-producer consumer

inorder to have producer we need to create a topic first 
open the cmd now in kafka/bin/window folder
kafka-topics.bat --create --bootstrap-server localhost:9092 --topic wordcount

producer
open command in same window folder
kafka-console-producer.bat --broker-list localhost:9092 --topic wordcount

for consumer open cmd in window folder
kafka-console-consumer.bat --topic wordcount --bootstrap-server localhost:9092 --from-beginning

now if we send any message it gonna appear in both

kafka-topics --list --bootstrap-server localhost:9092
