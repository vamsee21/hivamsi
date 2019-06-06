import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object HelloSpark {
  def main(args: Array[String]) {
    //val logFile = "file:///home/cloudera/Desktop/customer_data.txt" // Should be some file on your system
    val logFile = "hdfs://quickstart.cloudera:8020/user/cloudera/book.txt"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
