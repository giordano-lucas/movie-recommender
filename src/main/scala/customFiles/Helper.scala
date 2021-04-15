
package customFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// helper class
sealed class Helper(data:RDD[Rating]) {
  // variables first defined in Analyser
   val globalPred = 3.0
   val globalAvgRating = data.map(_.rating).mean() // average rating computation
   val user_ratings = data.groupBy(_.user)                   // groupby userId
                         .mapValues(Helper.meanRatings(_))   // mean of ratings
   val item_ratings = data.groupBy(_.item)                   // groupby itemId
                         .mapValues(Helper.meanRatings(_))   // mean of ratings
   // variables first defined in Predictor
   val avgItemDev = {
    // normalised deviation data set according to equation (2) of the M1 assignement description.
    val norm_data  = data.map(r => (r.user,r))
                      .join(user_ratings).values
                      .map {case (r,ru) => Rating(r.user,r.item, (r.rating - ru)/Helper.scale(r.rating,ru))}
    
    def l2_norm[T <: Iterable[Rating]] (rs:T) = math.sqrt(rs.map(r => r.rating * r.rating).sum / rs.size.toDouble)
    val prep_ratings = norm_data.groupBy(_.user)  // groupby userId
                            .flatMap {            // each Rating will be mapped to a new Rating with scaled rating 
                              case (_,rs) => rs.map(r => Rating(r.user,r.item, r.rating * l2_norm(rs)))
                              }   // mean of ratings
    val grouped:RDD[(Int,Map[Int,Double])] = prep_ratings.groupBy(_.user).mapValues(rs => (rs.map(r => (r.item,r.rating))).toMap)
    // cosine similarity
    /*val cosSim = (grouped cartesian grouped)                // consider all pairs of users
                  .filter(p => p._1._1 != p._2._1)          // except those with the same sure twice
                  .map { case ((u,uitems),(v,vitems)) => {  // compute the similarity for each pair
                    val sim = ((uitems.keySet & vitems.keySet) foldLeft 0.0) {(sum,i) => sum + uitems(i) * vitems(i)}
                    ((u,v), sim) }
                  }*/
    val cosSim = (grouped cartesian grouped)                // consider all pairs of users
                  .filter(p => p._1._1 != p._2._1)          // except those with the same sure twice
                  .map { case ((u,uitems),(v,vitems)) => {  // compute the similarity for each pair
                    val sim = ((uitems.keySet & vitems.keySet) foldLeft 0.0) {(sum,i) => sum + uitems(i) * vitems(i)}
                    (u, (v,sim))}
                  }.groupByKey().mapValues(_.toMap.withDefaultValue(0.0))
    
    /*val cosSim:RDD[(Int,Map[Int,Double])] =  grouped.map{case (u,uitems) => {// consider all pairs of users
        val block = grouped.filter(_._1 != u).mapValues{ 
                                        vitems => ((uitems.keySet & vitems.keySet).foldLeft(0.0)){(sum,i) => sum + uitems(i) * vitems(i)}
                                        }.collect.toMap.withDefaultValue(0.0)
        (u,block)
        }}*/              
    // user-specific weighted-sum deviation for all items
    (norm_data.groupBy(_.item) cartesian cosSim).map{
      case ((i,us),(u,sims)) => {
        val (num:Double,den:Double) = us.foldLeft((0.0,0.0)) {case ((num,den),Rating(v,_,r)) => (num + r * sims(v), den + sims(v).abs)}
        (i,(u, num / den))
      }}.groupByKey().mapValues(_.toMap)
    /*norm_data.groupBy(_.item).map{case (i,us) => {
        val block = cosSim.map {
          case (u,sims) => {
            val (num:Double,den:Double) = us.foldLeft((0.0,0.0)) {case ((num,den),Rating(v,_,r)) => (num + r * sims(v), den + sims(v).abs)}
            (u,num / den)
         }}.collect.toMap
        (i,block)
    }}*/ 
   }
}
// define companion object of Helper. Note that we are required to put the functions 'scale' and 'baselinePrediction'
// here to make them recognizable as serialisable for Spark.
object Helper {
  // define apply to avoid having to use the 'new' keyword
  def apply(data:RDD[Rating]) = new Helper(data)
  // construct baseline prediction
  def scale(x:Double,ru:Double) =
    if      (x > ru)  5-ru
    else if (x < ru)  ru-1
    else              1
  // variables first defined in Predictor
  def baselinePrediction(ru:Double,ri:Double)   = ru + ri * scale(ru+ri,ru)
  // mean function for lists of Ratings
  def meanRatings[T <: Iterable[Rating]] (rs:T) = rs.map(_.rating).sum / rs.size.toDouble
}

