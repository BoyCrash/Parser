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

    //esercizio 2.1)contare numero event per ogni actor
    //creo il DataFrame
    val dataFrameNumEvents = dataFrameEvent2.groupBy("actor").count()
    dataFrameNumEvents.show()
    //sulla creazione dell'RDD mi da un errore che non riesco a risolvere
    //lascio la riga di codice non funzionante

    //val rddActors = rddList.map(x => (x.actor, 1L))reduceByKey((count1, count2) => count1 + count2)
    //rddActors.take(10).foreach(println)

    //esercizio 2.2)contare il numero di event divisi per type e actor
    //creo il DataFrame
    val dataFrameEvents = dataFrameEvent2.select(($"type"), ($"actor"), count($"*")
      .over(Window.partitionBy("type", "actor")) as "nEvent")
    dataFrameEvents.show()
    //creo l'RDD
    val rddEvents = rddList.map(x => ((x.`type`, x.actor), 1L)).reduceByKey((e1,e2) => e1+e2)
    rddEvents.take(10).foreach(println)

    //esercizio 2.3)contare il numero di event divisi per type, actor e repo
    //creo il DataFrame
    val dataFrameEv = dataFrameEvent2.select($"type", $"actor", $"repo", count($"*")
      .over(Window.partitionBy($"type", $"actor", $"repo")) as "nEvent")
    dataFrameEv.show()
    //creo l'RDD
    val rddEv = rddList.map(x => ((x.`type`, x.actor, x.repo), 1L)).reduceByKey((e1,e2) => e1+e2)
    rddEv.take(10).foreach(println)

    //esercizio 2.4)contare gli event divisi per type, actor, repo e secondo trasformare timestamp
    //per avere solo il secondo valore, raggruppa su quest'ultimo
    //creo il DataFrame
    val dataFrameDate = dataFrameEvent2.withColumn("second", second($"created_at"))
    val dataFrameEve = dataFrameDate.select($"type", $"actor", $"repo", $"second", count($"*")
      .over(Window.partitionBy($"type", $"actor", $"repo", $"second")) as "nEvent")
    dataFrameEve.show()
    //creo l'RDD
    val rddEve = rddList.map(x=> ((x.`type`, x.actor, x.repo, new DateTime(x.created_at.getTime)
      .getSecondOfMinute), 1L))
      .reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    rddEve.take(10).foreach(println)

    //esercizio 2.5)trova max e min numero di event per secondo
    //creo il DataFrame dei massimi
    val dataFrameMaxDate = dataFrameEvent2.withColumn("second", second($"created_at"))
    val dataFrameMaxEv = dataFrameMaxDate.select($"second", count($"*")
      .over(Window.partitionBy($"second")) as "conteggio")
    val DataFrameMaxEve = dataFrameMaxEv.agg(max("conteggio"))
    DataFrameMaxEve.show()
    //creo l'RDD dei massimi
    val rddMaxDate = rddList.map(x=> (new DateTime(x.created_at.getTime)
      .getSecondOfMinute, 1L))
      .reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    val rddMaxDatee = rddMaxDate.map(x => x._2).max()
    println(rddMaxDatee)
    //creo il DataFrame dei minimi
    val dataFrameMinDate = dataFrameEvent2.withColumn("second", second($"created_at"))
    val dataFrameMinEv = dataFrameMinDate.select($"second", count($"*").over(Window.partitionBy($"second")) as "conteggio")
    val dataFrameMinEve = dataFrameMinEv.agg(min("conteggio"))
    dataFrameMinEve.show()
    //creo l'RDD dei minimi
    val rddMinDate = rddList.map(x=> (new DateTime(x.created_at.getTime)
      .getSecondOfMinute, 1L))
      .reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    val rddMinDatee = rddMinDate.map(x => x._2).min()
    println(rddMinDatee)

    //esercizio 2.6)trova max e min numero di event per actor
    //creo il DataFrame dei massimi
    val dataFramemaxActor = dataFrameEvent2.select($"actor", count($"*")
      .over(Window.partitionBy($"actor")) as "conteggio")
    val dataFrameMaxActors = dataFramemaxActor.agg(max("conteggio"))
    dataFrameMaxActors.show()
    //creo l'RDD dei massimi
    val rddmaxActor = rddList.map(x => (x.actor.id, x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMaxActors = rddmaxActor.map(x => x._2).max()
    println(rddMaxActors)
    //creo il DataFrame dei minimi
    val dataFrameMinActor = dataFrameEvent2.select($"actor", count($"*")
      .over(Window.partitionBy($"actor")) as "conteggio")
    val dataFrameMinActors = dataFrameMinActor.agg(min("conteggio"))
    dataFrameMinActors.show()
    //creo l'RDD dei minimi
    val rddMinActor = rddList.map(x => (x.actor.id, x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinActors = rddMinActor.map(x => x._2).min()
    println(rddMinActors)

    //esercizio 2.7)trova max e min numero di event per repo
    //creo il DataFrame dei massimi
    val dataFrameMaxRepo = dataFrameEvent2.select($"repo", count($"*")
      .over(Window.partitionBy($"repo")) as "conteggio")
    val dataFrameMaxRepos = dataFrameMaxRepo.agg(max("conteggio"))
    dataFrameMaxRepos.show()
    //creo l'RDD dei massimi
    val rddMaxRepo = rddList.map(x => (x.repo, x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMaxRepos = rddMaxRepo.map(x => x._2).max()
    println(rddMaxRepos)
    //creo il DataFrame dei minimi
    val dataFrameMinRepo = dataFrameEvent2.select($"repo", count($"*")
      .over(Window.partitionBy($"repo")) as "conteggio")
    val dataFrameMinRepos = dataFrameMinRepo.agg(min("conteggio"))
    dataFrameMinRepos.show()
    //creo l'RDD dei minimi
    val rddMinRepo = rddList.map(x => (x.actor.id, x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinRepos = rddMinRepo.map(x => x._2).min()
    println(rddMinRepos)

    //esercizio 2.8)trova max e min numero di event per secondo per actor
    //creo il DataFrame dei massimi
    val dataFrameAS = dataFrameEvent2.withColumn( "second", second($"created_at"))
    val dataFrameMaxAS = dataFrameAS.select( $"second", $"actor", count($"*")
      .over(Window.partitionBy( $"second", $"actor")) as "conteggio")
    val dataFrameMaxASS = dataFrameMaxAS.agg(max("conteggio"))
    dataFrameMaxASS.show()
    //creo l'RDD dei massimi
    val rddMaxActors = rddList.map(x => ((new DateTime(x.created_at.getTime)
      .getSecondOfMinute, x.actor.id), x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMaxActorsS = rddMaxActors.map(x => x._2).max()
    println(rddMaxActorsS)
    //creo il DataFrame dei minimi
    val dataFrameMinActors = dataFrameEvent2.withColumn( "second", second($"created_at"))
    val dataFrameMinAS = dataFrameMinActors.select( $"second", $"actor", count($"*")
      .over(Window.partitionBy( $"second", $"actor")) as "conteggio")
    val dataFrameMinASS = dataFrameMinAS.agg(min("conteggio"))
    dataFrameMinASS.show()
    //creo l'RDD dei minimi
    val rddMinActors = rddList.map(x => ((new DateTime(x.created_at.getTime)
      .getSecondOfMinute, x.actor.id), x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinAS = rddMinActors.map(x => x._2).min()
    println(rddMinAS)

    //esercizio 2.9)trova max e min numero di event per secondo per repo
    //creo il DataFrame dei massimi
    val dataFrameMaxRepo = dataFrameEvent2.withColumn( "second", second($"created_at"))
    val dataFrameMaxRepos = dataFrameMaxRepo.select( $"second", $"repo", count($"*")
      .over(Window.partitionBy( $"second", $"repo")) as "conteggio")
    val fdMaxRepos = dataFrameMaxRepos.agg(max("conteggio"))
    fdMaxRepos.show()
    //creo l'RDD dei massimi
    val rddMaxRepos = rddList.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo), x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMaxRepo = rddMaxRepos.map(x => x._2).max()
    println(rddMaxRepo)
    //creo il DataFrame dei minimi
    val dataFrameMinRepo = dataFrameEvent2.withColumn( "second", second($"created_at"))
    val dataFrameMinRepos = dataFrameMinRepo.select( $"second", $"repo", count($"*")
      .over(Window.partitionBy( $"second", $"repo")) as "conteggio")
    val dfminRepos = dataFrameMinRepos.agg(min("conteggio"))
    dfminRepos.show()
    //creo l'RDD dei minimi
    val rddMinRepo = rddList.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo), x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinRepos = rddMinRepo.map(x => x._2).min()
    println(rddMinRepos)

    //esercizio 2.10)trova max e min numero di event per secondo per repo e actor
    //creo il DataFrame dei massimi
    val dataFrameMaxRA = dataFrameEvent2.withColumn( "second", second($"created_at"))
    val dataFrameMaxRAS = dataFrameMaxRA.select( $"second", $"repo", $"actor", count($"*")
      .over(Window.partitionBy( $"second", $"repo", $"actor")) as "conteggio")
    val dataFrameMaxRASS = dataFrameMaxRAS.agg(max("conteggio"))
    dataFrameMaxRASS.show()
    //creo l'RDD dei massimi
    val rddMaxRA = rddList.map(x => ((new DateTime(x.created_at.getTime)
      .getSecondOfMinute, x.repo, x.actor.id), x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMaxRAS = rddMaxRA.map(x => x._2).max()
    println(rddMaxRAS)
    //creo il DataFrame dei minimi
    val dataFrameMinRA = dataFrameEvent2.withColumn( "second", second($"created_at"))
    val dataFrameMinRAS = dataFrameMinRA.select( $"second", $"repo", $"actor", count($"*")
      .over(Window.partitionBy( $"second", $"repo", $"actor")) as "conteggio")
    val dataFrameMinRASS = dataFrameMinRAS.agg(min("conteggio"))
    dataFrameMinRASS.show()
    //creo l'RDD dei minimi
    val rddMinRA = rddList.map(x => ((new DateTime(x.created_at.getTime)
      .getSecondOfMinute, x.repo, x.actor.id), x))
      .aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rddMinRAS = rddMinRA.map(x => x._2).min()
    println(rddMinRAS)

  }
}