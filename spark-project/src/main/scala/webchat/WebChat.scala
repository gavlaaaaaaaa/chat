package webchat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io._
import java.io._

object WebChat {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: WebChat <file>")
      System.exit(1)
    }

    val sconf = new SparkConf().setAppName("WebChat")
    val sc = new SparkContext(sconf)

    val sourcePos : InputStream = getClass.getResourceAsStream("/PositiveWords.txt")
    val positiveWords : Array[String] = scala.io.Source.fromInputStream(sourcePos).mkString.split("\n")

    val sourceNeg : InputStream = getClass.getResourceAsStream("/NegativeWords.txt")
    val negativeWords : Array[String] = scala.io.Source.fromInputStream(sourceNeg).mkString.split("\n")



    val file = args(0)
    val colList = sc.textFile(file).map(line => line.toLowerCase().split('|'))
    

    val groupedComments = colList.map(comments => (comments, webchat.replaceUnwantedWords(
                                                        webchat.standardiseString(comments(13))))). //remove commas out of comment
                                                map{case (row,words) => (row,words.split("\\W"). //split comment into words
                                                map(word => (word,1)))} //map each word into tuple with count of 1


    val count = groupedComments.map{case (row, counts) => (row.toList, counts.groupBy(wordTup => wordTup._1). // group by first element of tuple (word)
                                            map{case (key,tuples) => (key, tuples. //break out the set of feedback words per chat item
                                                                        map( wordTup => wordTup._2). //map to grab the count
                                                                        sum ) //add them al together
                                                }.
                                                map(words => webchat.classify(words,positiveWords,negativeWords)))}

    // output can be printed to show output before conversion to JSON
    //val output = count.map{case (row, counts) => (println(row.toList),println(counts.toList))}.collect()
    val date = System.currentTimeMillis / 1000
    println(date)
    val outfile = "/home/training/Documents/output/jsonout"+date

    val jsonOut = webchat.printMessageClassifications(count)

    sc.parallelize(List(jsonOut)).saveAsTextFile(outfile)
  }
}
