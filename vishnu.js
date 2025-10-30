spark-shell


import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD

val verArray = Array(
  (1L, ("Mumbai", 12442373)),
  (2L, ("Delhi", 11034555)),
  (3L, ("Bangalore", 8443675)),
  (4L, ("Hyderabad", 6993262))
)


val edgeArray = Array(
  Edge(1L, 2L, 1394),
  Edge(1L, 3L, 837),
  Edge(1L, 4L, 623),
  Edge(2L, 3L, 1741),
  Edge(2L, 4L, 1259),
  Edge(3L, 4L, 997)
)

val verRDD = sc.parallelize(verArray)

val edgeRDD = sc.parallelize(edgeArray)

val graph = Graph(verRDD, edgeRDD)


graph.numVertices

graph.numEdges

graph.inDegrees.collect()


graph.outDegrees.collect()

graph.degrees.collect()

graph.vertices.collect.foreach(println)

graph.edges.collect.foreach(println)





val triplets = graph.triplets.collect()

triplets.foreach(println)



graph.vertices.filter{case (id, (city,population)) => population > 8000000 }.collect()

graph.edges.filter{case Edge (city1, city2, distance) => distance < 700 }.collect()



val ranks = PageRank.run(graph, numIter = 10).vertices


val rankByCity = verRDD.join(ranks).map{case (id, ((city, population), rank)) => (city, rank)}

rankByCity.collect.foreach{ case (city, rank) => println(s"City : $city, Pagerank : $rank")}


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


  from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg

# ✅ Step 1: Spark Confhttps://codeshare.io/iguration
mysql_jar = "/home/cloudera/mysql-connector-java-5.1.34.jar"
conf = SparkConf().setAppName("MyApp").setMaster("local")
conf.set("spark.jars", mysql_jar)

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# ✅ Step 2: Database Connection Details
jdbc_url = "jdbc:mysql://localhost:3306/csee"
table_name = "emp"

properties = {
    "user": "root",
    "password": "cloudera",
    "driver": "com.mysql.jdbc.Driver"
}

# ✅ Step 3: Read data from MySQL table
df = sqlContext.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", table_name) \
    .option("user", "root") \
    .option("password", "cloudera") \
    .load()

# ✅ Step 4: Handle missing salary values
df = df.withColumn("salary", col("salary").cast("double"))
average_salary = df.select(avg("salary")).first()[0]
df = df.fillna({"salary": average_salary})

# ✅ Step 5: Write updated data back to MySQL
df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)

# ✅ Step 6: Stop Spark Context
sc.stop()
7
spark-submit --jars /home/cloudera/mysql-connector-java-5.1.49.jar sparkmysql.py
