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

  //val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
  // this is used to implicitly convert an RDD to a DataFrame.
  //import sqlContext.implicits._
  //val df = train.toDF()
  //************************************************
  var h = Helper(train,test)
  //************************************************
  
  def userItemDeviation(data:RDD[Rating],similarity:RDD[(Int,Map[Int,Double])]) = {
      def inner(us:Iterable[Rating], mapSim:Map[Int,Double]) = {
        val (num:Double,den:Double) = us.foldLeft((0.0,0.0)) {
          case ((num,den),Rating(v,_,r)) => (num + r * mapSim(v), den + mapSim(v).abs)
        }
        num / den         
      }  
      data.map(r => (r.user,r))
            .join(similarity).values
            .map{case (Rating(u,i,_),mapSim) => (i,(u,mapSim))}
            .join(h.scaled_train.groupBy(_.item))
            .map{case (i,((u,mapSim),us)) => ((u,i),inner(us,mapSim))}
    } 
  val cosSim  = h.cosSim.groupByKey().mapValues(_.toMap.withDefaultValue(0.0))
  def cos_dev = userItemDeviation(test,cosSim)

  def predict(dev:RDD[((Int,Int),Double)]):RDD[(Double, Double)] = 
                   test.map(r => ((r.user,r.item),r.rating))          // format pair for join on both item and users
                       .leftOuterJoin(dev)                            // perform left join to catch items without rating in train set and get item dev
                       .map{case ((u,_),value) => (u,value)}          // format pair for join on users
                       .join(h.user_ratings).values                   // then join on user avg ratings
                       .map{case ((r,opt_riu),ru) =>                  // compute baseline prediction    
                          (r, (opt_riu.map{Helper.baselinePrediction(ru,_)}.getOrElse(h.globalAvgRating)))
                        } // Note: output gloabal average if no item rating is available  */
                     
  // Question 2.3.1.
  def preds = predict(cos_dev)
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
                      
  def jaccard_mae = Helper.mae(predict(userItemDeviation(test,jaccardSim)))
  // Question 2.3.3.
  val number_user = train.groupBy(_.user).keys.count().toInt
  val nbSimComputations = (number_user * (number_user - 1)) // ??????? / 2
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
  // Question 2.3.6.
  val benchPred = Map[String,Double]().withDefaultValue(0.0)//benchmark(preds.count())
  // Question 2.3.7.
  val benchSim  = Map[String,Double]().withDefaultValue(0.0)//benchmark(h.cosSim.count())
  // ***************************************************
                
  def stats(series:Iterable[Double])(optN:Option[Int] = None):Map[String,Double] = {
    val n    = (optN getOrElse series.size).toDouble
    val mean = series.sum/n                           // mean of data
    val std  = series.map(x=>x*x).sum/n - mean*mean   // std of data 
    Map("min" -> series.min(Ordering.Double),  // Datatype of answer: Double
        "max" -> series.max(Ordering.Double),  // Datatype of answer: Double
        "average" -> mean,                     // Datatype of answer: Double
        "stddev" -> std)                       // Datatype of answer: Double
  }

  def benchmark[T](f: => T, epochs: Int = 10):Map[String,Double] = { // note the call by name parameter
    val series:Seq[Double] =            // series of measurements
      for {i <- 1 to epochs} yield {    // perform 'epochs' simulations
        h = Helper(train)               // force recomputation of fields inside helper 
        val s = System.nanoTime         // record initial time
        val ret = f                     // perform computation
        (System.nanoTime-s)/1e3         // compute the actual running time and convert from nano to micro
      }
    stats(series)(Some(epochs))
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
            "CosineSimilarityStatistics" -> stats(multiplications.map(_.toDouble))(None)
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
