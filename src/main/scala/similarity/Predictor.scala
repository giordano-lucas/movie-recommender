package similarity

import customFiles._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.TreeSet

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
  val h = Helper(train)
  
  def scale_rating(data:RDD[Rating]) =
    data.map(r => (r.user,r))
        .join(h.user_ratings).values
        .map {case (r,ru) => Rating(r.user,r.item, (r.rating - ru)/Helper.scale(r.rating,ru))}

  //************************************************
  // 2.1 Preprocessing Ratings
  val scaled_train  = scale_rating(train)
  def l2_norm[T <: Iterable[Rating]] (rs:T) = math.sqrt(rs.map(r => r.rating * r.rating).sum / rs.size.toDouble)
  val prep_ratings = scaled_train.groupBy(_.user)  // groupby userId
                          .flatMap {            // each Rating will be mapped to a new Rating with scaled rating 
                            case (_,rs) => rs.map(r => Rating(r.user,r.item, r.rating * l2_norm(rs)))
                            }   // mean of ratings
  //************************************************
  // cosine similarity
  val cosSim = prep_ratings.map(r => (r.item,r))
              .groupByKey()
              .flatMap{case (_,us) => 
                for {
                  Rating(u,_,ru) <- us
                  Rating(v,_,rv) <- us
                  if (u != v)
                  } yield ((u,v), ru * rv)
              }.reduceByKey(_ + _)
              .map{case ((u,v),sim) => (u,(v,sim))}
  //val users = prep_ratings.map(_.user).distinct
  //val all_user_pairs = (users cartesian users).map((_,None))
  //val allcosSim:RDD[(Int,(Int,Double))] = (cosSim rightOuterJoin all_user_pairs).map {case ((u,v),(opt_sim,_)) => (u,(v,opt_sim.getOrElse(0)))}
  
  //************************************************
  // user-specific weighted-sum deviation for all items
  def userItemDeviation(data:RDD[Rating],similarity:RDD[(Int,(Int,Double))]) = {
      val prep_train_join = scaled_train
            .map {case Rating(v,i,r) => ((v,i),r)}

      data.map(r => (r.user,r))
            .join(similarity).values
            .map{case (Rating(u,i,_),(v,sim)) => ((v,i),(u,sim))}
            //.leftOuterJoin(prep_train_join)
            //.map{case ((v,i),((u,sim),optr)) => ((u,i),(optr.getOrElse(0.0) * sim, sim.abs))}
            .join(prep_train_join)
            .map{case ((v,i),((u,sim),r)) => ((u,i),(r * sim, sim.abs))}
            //.map{case (Rating(u,i,_),(v,sim)) => ((u,i),(r * sim, sim.abs))}
            .reduceByKey {case ((num1,den1),(num2,den2)) =>((num1+num2),(den1 + den2))}
            .mapValues {case (num,den) => num / den}  
    }
  var cos_dev = userItemDeviation(test,cosSim)
  
  def predict(dev:RDD[((Int,Int),Double)]):RDD[(Double, Double)] = 
                   test.map(r => ((r.user,r.item),r.rating))          // format pair for join on both item and users
                       .leftOuterJoin(dev)                   // perform left join to catch items without rating in train set and get item dev
                       .map{case ((u,_),value) => (u,value)}          // format pair for join on users
                       .join(h.user_ratings).values                   // then join on user avg ratings
                       .map{case ((r,opt_riu),ru) =>                  // compute baseline prediction    
                          (r, (opt_riu.map{Helper.baselinePrediction(ru,_)}.getOrElse(h.globalAvgRating)))
                        } // Note: output gloabal average if no item rating is available  */
                      
  // report mae
  def mae(rdd: RDD[(Double,Double)]) =  // mean average error function
    rdd.map{ case (rat, pred) => (rat - pred).abs}.mean()
  // Question 2.3.1.
  val cosine_mae = mae(predict(cos_dev))
  // Question 2.3.2.
  def jaccard(a:Set[Int],b:Set[Int]):Double = {
    val inter = (a & b).size
    if (a.isEmpty || b.isEmpty) 0
    else inter.toDouble / (a.size + b.size - inter)
  }
  val user_set   = scaled_train.groupBy(_.user).mapValues(_.map(_.item).toSet).cache()
  val jaccardSim = (user_set cartesian user_set)
                      .filter {case ((u,_),(v,_))=> u!=v}
                      .map {case  ((u,a),(v,b)) => (u,(v,jaccard(a,b)))}
  val jaccard_mae = mae(predict(userItemDeviation(test,jaccardSim)))
  // Question 2.3.3.
  val number_user = train.groupBy(_.user).keys.count().toInt
  val nbSimComputations = (number_user * (number_user - 1)) // ??????? / 2
  // Question 2.3.4.
  val multiplications = prep_ratings.map(r => (r.item,r))
              .groupByKey()
              .flatMap{case (_,us) => 
                for {
                  Rating(u,_,ru) <- us
                  Rating(v,_,rv) <- us
                  if (u != v)
                  } yield ((u,v), 1)
              }.reduceByKey(_ + _).values
  
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
            "CosineSimilarityStatistics" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> 0 // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> 0.0, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> 0.0 // Datatype of answer: Double
          )
         )
    )
  Setup.terminate
}
