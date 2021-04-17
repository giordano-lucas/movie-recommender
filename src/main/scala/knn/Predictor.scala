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
  // **********************************************************************************************
  // **********************************************************************************************
  
  implicit def mapGetUpTo(map: Map[Int,(Double,Int)]) = new {
    def getUpTo(key: Int, maxk:Int): Double = {
      val (b,k) = map(key)
      if (k < maxk) b else 0.0
      } 
    }

  val ks   = List(10, 30, 50, 100, 200, 300, 400, 800, 943)
  val maxK = ks.max
  val h    = Helper(train,test)
  // all cosine Similarities 
  val cosSim:RDD[(Int,Map[Int,(Double,Int)])]  = 
    h.cosSim.groupByKey()
            .mapValues{  
                _.toList          // iterables cannot be sorted => list
                .sortBy(-_._2)   // descending sort on similarities
                .take(maxK)      // do not have to consider more that 943 neighbours
                .zipWithIndex    // add neighouring position to the map (index = i => ith closed neighbour)
                .map {case ((u,sim),idx) => (u,(sim,idx))} // format pair
                .toMap           // convert to Map
                .withDefaultValue((0.0,maxK)) // add default value of 0.0
              }.cache()
  
  def cos_dev = test             // same as in Similarity package
        .map(r => (r.user,r))
        .join(cosSim).values
        .map{case (Rating(u,i,_),mapSim) => (i,(u,mapSim))}
        .join(h.scaled_train.groupBy(_.item))
        .map{case (i,((u,mapSim),us)) => ((u,i), {
            // Implement equation (2) to compute the user-specific weighted-sum deviation for item i
            ks.map(k =>                             // compute riu for all values of k 
              us.foldLeft((0.0,0.0)) {              // start with (0,0)
                case ((num,den),Rating(v,_,r)) =>   
                    (num + r * mapSim.getUpTo(v,k), 
                    den + mapSim.getUpTo(v,k).abs)
                }
             ).map {case (num,den) => if (den == 0.0) 0.0 else num/den} 
        })}

  def kpred(opt_ls_riu:Option[List[Double]],ru:Double):List[Double] = 
    opt_ls_riu.map(ls_riu => ls_riu.map(Helper.baselinePrediction(ru,_)))
              .getOrElse(List.fill(ks.size)(h.globalAvgRating))
      
  val predictions:RDD[(Double,List[Double])] = 
    test.map(r => ((r.user,r.item),r.rating))      // format pair for join on both item and users
    .leftOuterJoin(cos_dev)                        // perform left join to catch items without rating in train set and get item dev
    .map{case ((u,_),value) => (u,value)}          // format pair for join on users
    .join(h.user_ratings).values                   // then join on user avg ratings
    .map{case ((r,opt_li_riu:Option[List[Double]]),ru) => // compute baseline prediction    
      (r, kpred(opt_li_riu,ru))
    } // Note: output gloabal average if no item rating is available  


  val maes = predictions
                .map{
                    case (rat, preds) => (preds.map(pred => (rat - pred).abs),1)
               }.reduce{ 
                    case ((l1,s1),(l2,s2)) => ((l1 zip l2).map{case(a,b) => a+b},s1+s2)}
              match { case (ls,s) => ls.map(_ / s) }
  //
  // question 3.2.2.
  //val nbSim = cos_dev.map(_.size).reduce(_ + _)
  val nb_users = train.groupBy(_.user).keys.count().toInt
  val ramSize:Long = (16 * 1 << 30)
  val kBytes = kBytes.map(k => (k,k * nb_users * (3 * 64 / 8)))
  val bytesPerUser = formatMaes("LowestKMaeMinusBaselineMae") * (3 * 64 / 8)
  // **********************************************************************************************
  // **********************************************************************************************
  // **********************************************************************************************
  def formatMaes:Map[String,Any] = {
    val zipped = ks.zip(maes)
    val out:Map[String,Double] = 
       zipped.map{case (k,mae) => (s"MaeForK=${k}",mae)}.toMap

    val (minK,minMae) = zipped.find(_._2 <= 0.7669).getOrElse((-1,0.0))
    out ++ List(
        "LowestKWithBetterMaeThanBaseline" -> minK,  
        "LowestKMaeMinusBaselineMae" -> (minMae - 0.7669)
    )
  }
  def formatBytes:Map[String,Double] =
       kBytes.map{ case(k,b) => (s"MinNumberOfBytesForK=${k}",b)}.toMap
  // Save answers as JSON
  Setup.outputAnswers(Map(
          "Q3.2.1" -> formatMaes,
          "Q3.2.2" -> formatBytes,
          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> ramSize, // Datatype of answer: Int
            "MaximumNumberOfUsersThatCanFitInRam" -> ramSize/bytesPerUser // Datatype of answer: Int
          )

          // Answer the Question 3.2.4 exclusively on the report.
         )
      )
  Setup.terminate
}
