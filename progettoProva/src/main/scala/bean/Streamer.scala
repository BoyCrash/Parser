
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.Timestamp
import java.text.SimpleDateFormat

object +Streamer {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkApplication").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val streamc = new StreamingContext(sc, Seconds(5))

    val filestream = streamc.textFileStream("C:\\BigData\\stream\\inputfolder")
    streamc.checkpoint("C:\\BigData\\checkpoint")

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
    val logs = filestream.flatMap(line => {
      val splitLine = line.split(",")
      List(Log(new Timestamp(dateFormat.parse(splitLine(0)).getTime), splitLine(1), splitLine(2).toInt))
    })

    val logsPair = logs.map(l => (l.tag, 1L))
    val resul = logsPair.reduceByKey((l1, l2) => l1 + l2)

    val function = (seqVal: Seq[Long], stateOpt: Option[Long]) => {
      stateOpt match {
        case Some(state) => Option(seqVal.sum + state)
        case None => Option(seqVal.sum)
      }
    }

    val logsPairTotal = logsPair.updateStateByKey(function)
    val joinedLogsPair = resul.join(logsPairTotal)

    //calcolare il numero di log per un tag, con finestra temporale di 20 secondi
    val resultWindow = logsPair.reduceByKeyAndWindow((l1,l2) => l1 + l2, Seconds(20))
    val joinedWindow = resultWindow.join(joinedLogsPair)
    joinedWindow.repartition(1).saveAsTextFiles("C:\\BigData\\stream\\outputfolder", "txt")

    streamc.start()
    streamc.awaitTermination()

  }
}