def loadCSV(path: String) : org.apache.spark.rdd.RDD[Array[String]] = { sc.textFile(path).map(_.split(",")) }

val pop = loadCSV("data/population.csv").map(arr => (arr(0), (arr(1), arr(2).toDouble)))
// sort by date descending
val sorted = pop.sortBy(kv => kv._2._1, false)
val grouped = sorted.groupByKey()
val countrySorted = grouped.sortByKey()
countrySorted.collect().foreach { case (k,v) => println(k); println(v) }
// secondary sort complete


// gonna find max pop for each region
val maxPops = countrySorted.map { case (k,v) => (k, v.toList.reduce((a, b) => if (a._2 >= b._2) a else b)) }
maxPops.collect().foreach { case (k,v) => println(k); println(v) }
// most will be 2015, lets find the ones that hit their peak earlier
val lessPops = maxPops.filter { case (_,v) => v._1 != "2015" }
lessPops.collect().foreach { case (k,v) => println(k); println(v) }
// sort ascending
val lessPopsSorted = lessPops.sortBy { case (k,v) => v._1 }

// and prettify into a table
val popFrame = lessPopsSorted.map { case (k,v) => (k,v._1,v._2) }.toDF("Country", "Year", "Pop")
popFrame.show()
