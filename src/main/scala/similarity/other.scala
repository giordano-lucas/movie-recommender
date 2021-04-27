package similarity

//val grouped:RDD[(Int,Map[Int,Double])] = prep_ratings.groupBy(_.user).mapValues(rs => (rs.map(r => (r.item,r.rating))).toMap)
    // cosine similarity
    /*val cosSim = (grouped cartesian grouped)                // consider all pairs of users
                  .filter(p => p._1._1 != p._2._1)          // except those with the same sure twice
                  .map { case ((u,uitems),(v,vitems)) => {  // compute the similarity for each pair
                    val sim = ((uitems.keySet & vitems.keySet) foldLeft 0.0) {(sum,i) => sum + uitems(i) * vitems(i)}
                    ((u,v), sim) }
                  }*/
                  /*
    val cosSim = (grouped cartesian grouped)                // consider all pairs of users
                  .filter(p => p._1._1 != p._2._1)          // except those with the same sure twice
                  .map { case ((u,uitems),(v,vitems)) => {  // compute the similarity for each pair
                    val sim = ((uitems.keySet & vitems.keySet) foldLeft 0.0) {(sum,i) => sum + uitems(i) * vitems(i)}
                    (u, (v,sim))}
                  }.groupByKey().mapValues(_.toMap.withDefaultValue(0.0))*/

  //val users = prep_ratings.map(_.user).distinct
  //val all_user_pairs = (users cartesian users).map((_,None))
  //val allcosSim:RDD[(Int,(Int,Double))] = (cosSim rightOuterJoin all_user_pairs).map {case ((u,v),(opt_sim,_)) => (u,(v,opt_sim.getOrElse(0)))}

  //************************************************
  // user-specific weighted-sum deviation for all items
 /*
  def userItemDeviation(data:RDD[Rating],similarity:RDD[(Int,Map[Int,Double])]) = {
      def inner(us:Iterable[Rating],
                mapSim:Map[Int,Double]) = {
        val (num:Double,den:Double) = us.foldLeft((0.0,0.0)) {case ((num,den),Rating(v,_,r)) => (num + r * mapSim(v), den + mapSim(v).abs)}
        num / den         
      }
          
      data.map(r => (r.user,r))
            .join(similarity).values
            .map{case (Rating(u,i,_),mapSim) => (i,(u,mapSim))}
            .join(scaled_train.groupBy(_.item))
            .map{case (i,((u,mapSim),us)) => ((u,i),inner(us,mapSim))}
    } 
    */
    
    //==========================================================================================
    //==========================================================================================
    //==========================================================================================

      /*def userItemDeviation(data:RDD[Rating],similarity:RDD[(Int,(Int,Double))]) = {
      val prep_train_join = scaled_train
            .map {case Rating(v,i,r) => ((v,i),r)}

      data.map(r => (r.user,r))
            .join(similarity).values
            .map{case (Rating(u,i,_),(v,sim)) => ((v,i),(u,sim))}
            //.leftOuterJoin(prep_train_join)
            //.map{case ((v,i),((u,sim),optr)) => ((u,i),(optr.getOrElse(0.0) * sim, sim.abs))}
            .join(prep_train_join)
            .map{case ((v,i),((u,sim),r)) => ((u,i),(r * sim, sim.abs))}
            .reduceByKey {case ((num1,den1),(num2,den2)) =>((num1+num2),(den1 + den2))}
            .mapValues {case (num,den) => num / den}  
    }*/

    //=======================================================================================

package similarity

import customFiles._
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
  // **********************************************************************************************
  // **********************************************************************************************

  //************************************************
  var h = Helper(train,test)
  //************************************************
  // Question 2.3.1.
  def userItemDeviation(similarity:RDD[(Int,Map[Int,Double])]) = {
      def inner(us:Iterable[Rating], mapSim:Map[Int,Double]) = {
        val (num:Double,den:Double) = us.foldLeft((0.0,0.0)) {
          case ((num,den),Rating(v,_,r)) => (num + r * mapSim(v), den + mapSim(v).abs)
        }
        num / den         
      }  
      test.map(r => (r.user,r))
            .join(similarity).values
            .map{case (Rating(u,i,_),mapSim) => (i,(u,mapSim))}
            .join(h.scaled_train.groupBy(_.item))
            .map{case (i,((u,mapSim),us)) => ((u,i),inner(us,mapSim))}
    } 
  def predict(dev:RDD[((Int,Int),Double)]):RDD[(Double, Double)] = 
                   test.map(r => ((r.user,r.item),r.rating))          // format pair for join on both item and users
                       .leftOuterJoin(dev)                            // perform left join to catch items without rating in train set and get item dev
                       .map{case ((u,_),value) => (u,value)}          // format pair for join on users
                       .join(h.user_ratings).values                   // then join on user avg ratings
                       .map{case ((r,opt_riu),ru) =>                  // compute baseline prediction    
                          //(r, (opt_riu.map{Helper.baselinePrediction(ru,_)}.getOrElse(h.globalAvgRating)))
                          (r, Helper.baselinePrediction(ru,(opt_riu getOrElse 0.0)))
                        } // Note: output gloabal average if no item rating is available 
  
  // Question 2.3.6. and Question 2.3.7.
  val (preds,benchSim,benchPred) = benchmark()
  // Question 2.3.1.
  def cosine_mae = Helper.mae(preds)
  // Question 2.3.2.
  def jaccard(a:Set[Int],b:Set[Int]):Double = {
    val inter = (a & b).size
    if (a.isEmpty || b.isEmpty) 0
    else inter.toDouble / (a.size + b.size - inter)
  }
  val user_set   = h.scaled_train.groupBy(_.user).mapValues(_.map(_.item).toSet).cache()
  val jaccardSim = (user_set cartesian user_set)
                      .filter {case ((u,_),(v,_))=> u!=v}
                      .map {case  ((u,a),(v,b)) => (u,(v,jaccard(a,b)))}
                      .groupByKey().mapValues(_.toMap.withDefaultValue(0.0))
                      
  def jaccard_mae = Helper.mae(predict(userItemDeviation(jaccardSim)))
  // Question 2.3.3.
  val number_user = train.groupBy(_.user).keys.count().toInt
  val nbSimComputations = number_user * number_user
  // Question 2.3.4.
  val multiplications = h.prep_ratings.map(r => (r.item,r))
              .groupByKey()
              .flatMap{case (_,us) => 
                for {
                  Rating(u,_,ru) <- us
                  Rating(v,_,rv) <- us
                  if (u != v)
                  } yield ((u,v), 1)
              }.reduceByKey(_ + _).values.collect()
  // Question 2.3.5.
  val bytesSimilarities = multiplications.size * (64 / 8)
  // **********************************************************************************************
  // **********************************************************************************************
  // **********************************************************************************************
  // benchmark functions
                
  def stats(series:Iterable[Double]):Map[String,Double] = {
    val n    = series.size
    val mean = series.sum/n                           // mean of data
    val std  = series.map(x=>x*x).sum/n - mean*mean   // std of data 
    Map("min" -> series.min(Ordering.Double),  // Datatype of answer: Double
        "max" -> series.max(Ordering.Double),  // Datatype of answer: Double
        "average" -> mean,                     // Datatype of answer: Double
        "stddev" -> std)                       // Datatype of answer: Double
  }
  def time[T](f:RDD[T]):Double = {
      val s = System.nanoTime  // record initial time
      f.count()                // perform action on RDD to force computation
      (System.nanoTime-s)/1e3  // compute the actual running time and convert from nano to micro
  }
  def benchmark(epochs: Int = 5):(RDD[(Double, Double)],Map[String,Double],Map[String,Double]) = { // note the call by name parameter
    var preds = spark.sparkContext.emptyRDD[(Double,Double)]
    val series:Seq[(Double,Double)] =   // series of measurements
      for {i <- 1 to epochs} yield {    // perform 'epochs' simulations
        h = Helper(train)               // force recomputation of fields inside helper 
        // *** Similarities
        val cosSim  = h.cosSim
                       .groupByKey()
                       .mapValues(_.toMap.withDefaultValue(0.0))
                       .cache() // perform computation
        val similarityTime = time(cosSim)
        // *** Predictions
        val cos_dev = userItemDeviation(cosSim)
        preds.unpersist() // un cache previous iteration
        preds = predict(cos_dev).cache()
        val predictionTime = time(preds)
        cosSim.unpersist()// un cache previous iteration
        (similarityTime,predictionTime)
      }
    val (similarityTimes,predictionTimes) = series.unzip
    // return type
    (preds,stats(similarityTimes),stats(predictionTimes))
  }

  // **********************************************************************************************
  // **********************************************************************************************
  // **********************************************************************************************

  Setup.outputAnswers(Map(
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> cosine_mae, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> (cosine_mae-0.7669)// Datatype of answer: Double
          ),
          "Q2.3.2" -> Map(
            "JaccardMae" -> jaccard_mae, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> (jaccard_mae-cosine_mae) // Datatype of answer: Double
          ),
          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> nbSimComputations // Datatype of answer: Int
          ),
          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> stats(multiplications.map(_.toDouble))
          ),
          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> bytesSimilarities // Datatype of answer: Int
          ),
          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> benchPred
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),
          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> benchSim,
            "AverageTimeInMicrosecPerSuv" -> benchSim("average") / multiplications.size, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> benchSim("average") / benchPred("average") // Datatype of answer: Double
          )
         )
    )
  Setup.terminate
}
