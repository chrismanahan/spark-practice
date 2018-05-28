spark-shell --packages com.databricks:spark-csv_2.11:1.0.3


class CSVHeader(header: Array[String]) extends Serializable {
     val index = header.zipWithIndex.toMap
     def apply(array: Array[String], key: String) : String = array(index(key))
}

def csvTup(path: String) : Array[String]) = {
	val csv = sc.textFile(path)
	return csv.map(line => line.split(","))
}

val csv = sc.textFile("data/weather.csv")
val data = csv.map(line => line.split(",").map(element => element.trim))
val header = new CSVHeader(data.take(1)(0))
val rows = data.filter(line => header(line, "Date") != "Date")


val tups = rows.map(arr => (arr(0), arr.lift(1)))

------------------------------

case class StockKey(ticker: String, date: String)

object StockKey {
    implicit def orderingByTickerDate[A <: StockKey] : Ordering[A] = {
        Ordering.by(key => (key.ticker, key.date))
    }
}

def makeKeyVal(data: Array[String]) : (StockKey, String) = { (compositeKey(data), data(2)) }
def compositeKey(data: Array[String]) : StockKey = { StockKey(data(0), data(1)) }

class StockPartitioner(partitions: Int) extends Partioner {
    require(paritions >= 0)

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[StockKey]
        k.ticker.hashCode() % numPartitions
    }
}


val stocks = sc.textFile("data/stock_sample.csv").map(line => line.split(","))
val keyedData = stocks.map(makeKeyVal)
val sortedData = keyedData.repartitionAndSortWithinPartitions(new StockPartitioner(1))


-----------------------------------

def loadCSV(path: String) : org.apache.spark.rdd.RDD[Array[String]] = { sc.textFile(path).map(_.split(",")) }

val users = loadCSV("data/users.csv").map(arr => (arr(0), arr(1))).toDF("user_id", "loc_id")
val txs = loadCSV("data/transactions.csv").map(arr => (arr(2), arr(1))).toDF("user_id", "pid")

val joinedData = users.join(txs, Seq("user_id"), "left_outer").filter(row => row(2) != null)
val prodLoc = joinedData.map(row => (row.getString(2), row.getString(1)))
val prodSets = prodLoc.rdd.map(tup => (tup._1, scala.collection.mutable.Set[String](tup._2)))
val output = prodSets.reduceByKey((a,b) => a.union(b)).map(tup => (tup._1, tup._2.size))
output.collect()

