package knn

import customFiles._
import similarity._
import org.apache.spark.rdd.RDD

class Conf(arguments: Seq[String]) extends JsonConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  verify()
}
object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  implicit val spark = Setup.initialise
  implicit val conf  = new Conf(args) 
  println("")
  println("******************************************************")
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train     = Setup.readFile(trainFile)
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test     = Setup.readFile(testFile)

  // **********************************************************************************************
  // ******************************** Explanation of implementation *******************************
  // **********************************************************************************************
  "In order to avoid large computation time, I decided to avoid having to run the code once for each"     +
  "value of k. Hence, I rather chose to modify the Map data structure I used in the Similarity.Predictor" +
  "class. Intead of storing a Map of type User => Similarity, I change the value type to (Similarity,sortOrder)" +
  "where sortOrder is the index of the similarity value in the sorted array."
  /* Example: 
        Map(
            1->1.0,
            2->0.1,
            3->0.7,
            4->-0.2
          ) 
    becomes 
         Map(
            1->(1.0,0),
            2->(0.1,2),
            3->(0.7,1),
            4->(-0.2,3)
          ) */
  "This allows to refine the get method on map to take the sortOrder value into consideration"
  // this function is define to substitute to the apply function of a Map
  // On top of the key, it takes a maxk parameter
  // if (k < maxk) we output the same as what the map would have outputed
  // otherwise we return 0.
  implicit def mapGetUpTo(map: Map[Int,(Double,Int)]) = new {
    def getUpTo(key: Int, maxk:Int): Double = {
      val (b,k) = map(key)
      if (k < maxk) b else 0.0
      } 
    }
  "It should be now fairly clear that I can compute all maes at the same time using a single map as " +
  "it was done in the previous questions."
  // **********************************************************************************************
  // ************************************ Actual Implementation  **********************************
  // **********************************************************************************************

  // values of k to be considered
  val ks   = List(10, 30, 50, 100, 200, 300, 400, 800, 943)
  val maxK = ks.max 
  // helper as always
  val h    = Helper(train,test)
  // all cosine Similarities 
  val cosSim:RDD[(Int,Map[Int,(Double,Int)])]  = 
    h.cosSim.groupByKey()        // start from cosine similarity of Similarity.Predictor.
            .mapValues {         // 
                _.toList         // iterables cannot be sorted => list
                .sortBy(-_._2)   // descending sort on similarities
                .take(maxK)      // do not have to consider more that 943 neighbours
                .zipWithIndex    // add neighouring position to the map (index = i => ith closed neighbour)
                .map {case ((u,sim),idx) => (u,(sim,idx))} // format pair
                .toMap           // convert to Map
                .withDefaultValue((0.0,maxK)) // add default value of 0.0
              }.cache()
  // user-specific weighted-sum deviation for all items
  def cos_dev = test             // same as in Similarity package
        .map(r => (r.user,r))
        .join(cosSim).values
        .map{case (Rating(u,i,_),mapSim) => (i,(u,mapSim))}
        .join(h.scaled_train.groupBy(_.item))
        .map{case (i,((u,mapSim),us)) => ((u,i), {
            // Implement equation (2) to compute the user-specific weighted-sum deviation for item i
            ks.map(k =>                             // compute riu for all values of k 
              us.foldLeft((0.0,0.0)) {              // start with (0,0)
                case ((num,den),Rating(v,_,r)) =>   // iteratively construct the numerator and denominator for equation (2)
                    (num + r * mapSim.getUpTo(v,k), // numerator computation
                    den + mapSim.getUpTo(v,k).abs)  // denominator computation
                } // finaly we can compute the deviation values
             ).map {case (num,den) => if (den == 0.0) 0.0 else num/den} 
        })}
  // perform prediction for all values of k                 
  def kpred(opt_ls_riu:Option[List[Double]],ru:Double):List[Double] = 
    opt_ls_riu.map(ls_riu => ls_riu.map(Helper.baselinePrediction(ru,_)))  // prediction computation (equation (3) of assignement description) 
              .getOrElse(List.fill(ks.size)(ru))                           // return user average if we got have a item/user deviation
      
  val predictions:RDD[(Double,List[Double])] = 
    test.map(r => ((r.user,r.item),r.rating))      // format pair for join on both item and users
    .leftOuterJoin(cos_dev)                        // perform left join to catch items without rating in train set and get item dev
    .map{case ((u,_),value) => (u,value)}          // format pair for join on users
    .join(h.user_ratings).values                   // then join on user avg ratings
    .map{case ((r,opt_li_riu:Option[List[Double]]),ru) => // compute baseline prediction    
      (r, kpred(opt_li_riu,ru))
    } // Note: output gloabal average if no item rating is available  

  val maes = predictions  // compute the k meas
                .map{     // map each pred_k => | pred_k - actual rating |
                 case (rat, preds) => (preds.map(pred => (rat - pred).abs),1)
               }.reduce{  // simply sum all small terms
                    case ((l1,s1),(l2,s2)) => ((l1 zip l2).map{case(a,b) => a+b},s1+s2)
               } match { case (ls,s) => ls.map(_ / s) } // divide to get average maes
  // question 3.2.2.
  val nb_users     = train.groupBy(_.user).keys.count().toInt
  val ramSize:Long = (16 * 1L << 30)
  def memory(k:Int,nb_users:Int=nb_users) = k * nb_users * (64 / 8)
  val kBytes       = ks.map(k => (k,memory(k)))
  // use sparse matrix implementation that used 3x the memory than what we computed before 
  val bytesPerUser = 3 * memory(formatMaes("LowestKWithBetterMaeThanBaseline").asInstanceOf[Int],1)
  // **********************************************************************************************
  // **********************************************************************************************
  // **********************************************************************************************
  // format k maes into the output json format
  def formatMaes:Map[String,Any] = {
    val zipped = ks.zip(maes)
    val out:Map[String,Any] = 
       zipped.map{case (k,mae) => (s"MaeForK=${k}",mae)}.toMap

    val (minK:Int,minMae) = zipped.find(_._2 <= 0.7669).getOrElse((-1,0.0))
    out ++ List(
        "LowestKWithBetterMaeThanBaseline" -> minK,  
        "LowestKMaeMinusBaselineMae" -> (minMae - 0.7669)
    )
  } // format the output into json for the memory question
  def formatBytes:Map[String,Int] =
       kBytes.map{ case(k,b) => (s"MinNumberOfBytesForK=${k}",b)}.toMap
  // **********************************************************************************************
  // **********************************************************************************************
  // **********************************************************************************************
  // Save answers as JSON
  Setup.outputAnswers(Map(
          "Q3.2.1" -> formatMaes,
          "Q3.2.2" -> formatBytes,
          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> ramSize, // Datatype of answer: Long
            "MaximumNumberOfUsersThatCanFitInRam" -> ramSize/bytesPerUser // Datatype of answer: Long
          )

          // Answer the Question 3.2.4 exclusively on the report.
         )
      )
  Setup.terminate
}
