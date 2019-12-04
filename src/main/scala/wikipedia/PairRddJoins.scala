package wikipedia

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PairRddJoins {
  val sc: SparkContext = WikipediaRanking.sc

  sealed trait Abonnement
  case object AG extends Abonnement
  case object DemiTarif extends Abonnement
  case object DemiTarifVisa extends Abonnement

  private val as: Seq[(Int, (String, Abonnement))] = List(
    (101, ("Ruetli", AG)),
    (102, ("Brelaz", DemiTarif)),
    (103, ("Grees", DemiTarifVisa)),
    (104, ("Shatten", DemiTarif))
  )

  private val abos: RDD[(Int, (String, Abonnement))] = sc.parallelize(as)

  private val ls = List(
    (101, "Bern"),
    (101, "Thun"),
    (102, "Lausanne"),
    (102, "Geneve"),
    (102, "Nyon"),
    (103, "Zurich"),
    (103, "St-Gallen"),
    (103, "Chur"),
  )

  private val locations: RDD[(Int, String)] = sc.parallelize(ls)

  private val trackedCustomers = abos
    .join(locations)
    .map {
      case (id, ((name, subscription), location)) => (id, (name, subscription))
    }
    .distinct

  private val abosWithOptionalLocations = abos
    .leftOuterJoin(locations)
    .filter {
      case (_, ((_, _), location)) => location.isDefined
    }
    .map {
      case (id, ((name, subscription), location)) => (id, (name, subscription))
    }
    .distinct

  def main(args: Array[String]): Unit = {
//    trackedCustomers.collect.sortBy { case (key, _) => key }.foreach(println)

    abosWithOptionalLocations.collect
      .sortBy { case (key, _) => key }
      .foreach(println)
  }
}
