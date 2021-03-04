import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, explode, max, min, second, sum}
import org.apache.spark.sql.{Row, SQLContext, functions}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import java.util.Date
import scala.math.Ordering.{Tuple2, comparatorToOrdering}
import scala.tools.scalap.scalax.rules.scalasig.ScalaSigEntryParsers.entryType

object JsonParser {

  val conf = new SparkConf().setMaster("local[2]").setAppName("TabellaQuery")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)
  val input = "C:\\BigData\\ProgettoBigData\\FirstRows.json"

  import hiveContext.implicits._
  def main(args: Array[String]){

    //qui faccio il parsing del file
    val dataFrameEvent = sqlContext.read.json(input)
    val dataFrameEvent2 = dataFrameEvent.withColumnRenamed("public", "publicField")
    //creo dataSet
    val dataSetEvent = dataFrameEvent2.as[Event]
    //creo rdd
    val rddList = dataSetEvent.rdd

    //esercizio 1.1)trovare i singoli actor
    //qui creo il DataFrame
    val dataFrameActor = dataFrameEvent2.select("actor").distinct()
    dataFrameActor.show()
    //qui invece creo l'RDD
    val rddActor = rddList.map(x => x.actor).distinct()
    rddActor.take(10).foreach(println)

    //esercizio 1.2)trovare i singoli author dentro commit
    //aggiornamento sul DataFrame: cambio da event a commit
    val dataFramepayload = dataFrameEvent.select("payload.*")
    val dataFrameCommit = dataFramepayload.select(explode(col("commits"))).select("col.*")
    val dataFrameAuthor = dataFrameCommit.select("author").distinct()
    dataFrameAuthor.show()
    //qui creo l'RDD
    val rddCommit = dataFrameCommit.as[Commit].rdd
    val rddAuthor = rddCommit.map(x => x.author).distinct()
    rddAuthor.take(10).foreach(println)

    //esercizio 1.3)trovare i singoli repo
    //creo il DataFrame
    val dataFrameRepository = dataFrameEvent2.select("repo").distinct()
    dataFrameRepository.show()
    //creazione RDD
    val rddRepository = rddList.map(x => x.repo).distinct()
    rddRepository.take(10).foreach(println)

    //esercizio 1.4)trovare i vari tipi di evento type
    //creo il DataFrame
    val dataFrameType = dataFrameEvent2.select("`type`").distinct()
    dataFrameType.show()
    //creo l'RDD
    val rdd_type = rddList.map(x => x.`type`).distinct()
    rdd_type.take(10).foreach(println)

    //esercizio 1.5)contare il numero di actor
    //creo il DataFrame
    val dataFrameActor = dataFrameEvent2.select("actor").distinct().count()
    println(dataFrameActor)
    //creo l'RDD
    val rddActor = rddList.map(x => x.actor).distinct().count()
    println(rddActor)

    //esercizio 1.6)contare il numero di repo
    //creo il DataFrame
    val dataFrameRepo = dataFrameEvent2.select("repo").distinct().count()
    println(dataFrameRepo)
    //creo l'RDD
    val rddRepo = rddList.map(x => x.repo).distinct().count()
    println(rddRepo)

  }
}