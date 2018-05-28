def loadCSV(path: String) : org.apache.spark.rdd.RDD[Array[String]] = { sc.textFile(path).map(_.split(",")) }

val users = loadCSV("data/users.csv").map(arr => (arr(0), arr(1))).toDF("user_id", "loc_id")
val txs = loadCSV("data/transactions.csv").map(arr => (arr(2), arr(1))).toDF("user_id", "pid")

val joinedData = users.join(txs, Seq("user_id"), "left_outer").filter(row => row(2) != null)
val prodLoc = joinedData.map(row => (row.getString(2), row.getString(1)))
val prodSets = prodLoc.rdd.map(tup => (tup._1, scala.collection.mutable.Set[String](tup._2)))
val output = prodSets.reduceByKey((a,b) => a.union(b)).map(tup => (tup._1, tup._2.size))
output.collect()