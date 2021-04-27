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
  // create helper to reuse some computation accross classes as done in Milestone 1
  var h = Helper(train,test)
  //  compute cosine similarites (equation (1)). Look at the Helper.scala file for more details
  val cosSim  = h.cosSim.groupByKey().mapValues(_.toMap.withDefaultValue(0.0))
  // Implement equation (2) : the user-specific weighted-sum deviation for item i 
  def userItemDeviation(similarity:RDD[(Int,Map[Int,Double])]) = {
      // implement equation (2) for a fixed i, given a list of ratings and the corresponding similarities
      def inner(us:Iterable[Rating], mapSim:Map[Int,Double]) = {
        val (num,den) = us.foldLeft((0.0,0.0)) // simple foldLeft to compute both the numerator and denumerator of equation (2)
            {case ((num,den),Rating(v,_,r)) => (num + r * mapSim(v), den + mapSim(v).abs)}
        num / den  // compute ratio  
      } // now we can perform the real computation for the whole test set
      test.map(r => (r.user,r))                                        // map to prepare join on user
            .join(similarity).values                                   // get list of non-zero similarities for user u 
            .map{case (Rating(u,i,_),mapSim) => (i,(u,mapSim))}        // prepare join on items
            .join(h.scaled_train.groupBy(_.item))                      // get list of ratings of item i 
            .map{case (i,((u,mapSim),us)) => ((u,i),inner(us,mapSim))} // use inner ft to compute the deviation for all (u,i) pairs
    } // compute the deviation using cosine similarities
  def cos_dev = userItemDeviation(cosSim)
  // define the prediction function. Essentially the same as milestone 1 but joining on user-item deviation first
  def predict(dev:RDD[((Int,Int),Double)]):RDD[(Double, Double)] = 
                   test.map(r => ((r.user,r.item),r.rating))          // format pair for join on both item and users
                       .leftOuterJoin(dev)                            // perform left join to catch items without without user-item dev
                       .map{case ((u,_),value) => (u,value)}          // format pair for join on users
                       .join(h.user_ratings).values                   // then join on user avg ratings
                       .map{case ((r,opt_riu),ru) =>                  // implement equation (3)
                         (r, Helper.baselinePrediction(ru,(opt_riu getOrElse 0.0))) 
                        } // Note: the default value for (u,i)-dev is zero wihch amounts to predict ru    
  // Question 2.3.1.
  def preds = predict(cos_dev)        // compute actual predictions
  def cosine_mae = Helper.mae(preds)  // compute associated mean absolute error
  // Question 2.3.2.
  def jaccard(a:Set[Int],b:Set[Int]):Double = {       // simple jaccard index computation
    val inter = (a & b).size                          // set intersection size
    if (a.isEmpty || b.isEmpty) 0                     // edge
    else inter.toDouble / (a.size + b.size - inter)   // ratio 
  }
  // compute RDD : user => set(item)
  val user_set   = h.scaled_train                     // scaled training set
                      .groupBy(_.user)                // group by user to get list items
                      .mapValues(_.map(_.item).toSet) // convert list to set
                      .cache()                        // cache since it is reused by question 2.3.4.
  // compute all jaccard similarities
  val jaccardSim = (user_set cartesian user_set)                              // get all pairs of users 
                      .filter {case ((u,_),(v,_))=> u!=v}                     // filter same users pair
                      .map {case  ((u,a),(v,b)) => (u,(v,jaccard(a,b)))}      // compute jaccard sim between each pair
                      .groupByKey().mapValues(_.toMap.withDefaultValue(0.0))  // same as for cosine similarities                    
  def jaccard_mae = Helper.mae(predict(userItemDeviation(jaccardSim)))        // compute mae
  // Question 2.3.3.
  val number_user = train.groupBy(_.user).keys.count().toInt  // get the number of users in the dataset
  val nbSimComputations = number_user * number_user           // all similarties (formula in the report)
  // Question 2.3.4.
  val multiplications = (user_set cartesian user_set)             // reuse set dataframe of jaccard sim
                      .map {case  ((_,a),(_,b)) => (a & b).size}  // nb multiplications = |A inter B|
                      .collect()                                  // get list of data
  // Question 2.3.5.
  val bytesSimilarities = multiplications.size * (64 / 8)         // formula in the report
  // Question 2.3.6.
  val benchPred = benchmark(preds.count()) //Map[String,Double]().withDefaultValue(0.0)// benchmark(preds.count())
  // Question 2.3.7.
  val benchSim  = benchmark(h.cosSim.count()) //Map[String,Double]().withDefaultValue(0.0)//benchmark(h.cosSim.count())
  // **********************************************************************************************
  // ********************************* benchmark functions ****************************************
  // **********************************************************************************************
                
  def stats(series:Iterable[Double]):Map[String,Double] = {      // compute various stats from series of measurements
    val n    = series.size.toDouble                              // convert to double to avoid integer division
    val mean = series.sum/n                                      // mean of data
    val std  = math.sqrt(series.map(x=>x*x).sum/n - mean*mean)   // std of data 
    Map("min" -> series.min(Ordering.Double),  // Datatype of answer: Double
        "max" -> series.max(Ordering.Double),  // Datatype of answer: Double
        "average" -> mean,                     // Datatype of answer: Double
        "stddev" -> std)                       // Datatype of answer: Double
  }

  def benchmark[T](f: => T, epochs: Int = 5):Map[String,Double] = { // note the call by name parameter
    val series:Seq[Double] =            // series of measurements
      for {i <- 1 to epochs} yield {    // perform 'epochs' simulations
        h = Helper(train)               // force recomputation of fields inside helper 
        val s = System.nanoTime         // record initial time
        val ret = f                     // perform computation
        (System.nanoTime-s)/1e3         // compute the actual running time and convert from nano to micro
      }
    stats(series)
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
