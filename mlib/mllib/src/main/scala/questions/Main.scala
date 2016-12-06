package questions

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

  def main(args: Array[String]) = {

  	Logger.getLogger("org").setLevel(Level.OFF)
  	Logger.getLogger("akka").setLevel(Level.OFF)

    val processor = new GamingProcessor()
    
    // load the data and tranform it
    val data = processor.convert(getClass().getResource("/osge_pool-1-thread-1.data").toString.drop(5))

    val indexed = processor.indexer(data)

    val preprocessed = processor.toLabeledPoints(indexed)

    //split the dataset into training(90%) and prediction(10%) sets
    val splitted = preprocessed.randomSplit(Array(0.9,0.1), 10L)

    //train the model
    val model = processor.createModel(splitted(0))

    //predict
    val predictions = processor.predict(model, splitted(1))
    

    //calculate how many correct predictions
    val total = predictions.count()
    val correct = splitted(1).zip(predictions).filter(x => x._1.label == x._2).count()
    
    println(Console.YELLOW+"Predicted correctly: " +Console.GREEN+ (correct.toDouble/total)*100 + "%")
    
    //finally stop sparkContext
    processor.sc.stop()
  }
}