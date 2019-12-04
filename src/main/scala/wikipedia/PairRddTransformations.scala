package wikipedia

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PairRddTransformations {
  val sc: SparkContext = WikipediaRanking.sc

  val pairRdd: RDD[(String, String)] =
    WikipediaRanking.wikiRdd.map(wa => (wa.title, wa.text))

  case class Event(organizer: String, name: String, budget: Int)

  val events: Seq[Event] = Event("organizer1", "name1", 100) ::
    Event("organizer2", "name2", 200) :: Event("organizer3", "name3", 300) :: Nil

  val eventsRdd: RDD[(String, Int)] =
    sc.parallelize(events).map(event => (event.organizer, event.budget))

  val budgedRdd: RDD[(String, Int)] = eventsRdd.reduceByKey(_ + _)

  val intermediate: RDD[(String, (Int, Int))] = eventsRdd
    .mapValues((_, 1))
    .reduceByKey(
      (left: (Int, Int), right: (Int, Int)) =>
        (left._1 + right._1, left._2 + right._2)
    )

  val avgBudgets: RDD[(String, Int)] = intermediate.mapValues {
    case (budget, nrOfEvents) => budget / nrOfEvents
  }

  case class Visitor(ip: String, timestamp: String, duration: String)

  val visitors = Visitor("192.168.1.1", "11111111", "1") ::
    Visitor("192.168.1.2", "11111122", "2") ::
    Visitor("192.168.1.3", "11111133", "2") ::
    Visitor("192.168.1.4", "111111144", "3") :: Nil

  val visits: RDD[Visitor] = sc.parallelize(visitors)

  val numUniqueVisits1: collection.Map[String, Long] =
    visits.map(visit => (visit.ip, visit.duration)).countByKey

  val numUniqueVisitsImproved2e: Long = visits.map(_.ip).distinct.count
}
