package webchat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io._
import java.io._
import org.apache.spark.rdd.RDD


package object webchat {

	val sourceProf : InputStream = getClass.getResourceAsStream("/Profanity.txt")
	val profanityWords : Array[String] = scala.io.Source.fromInputStream(sourceProf).mkString.split("\n")
	
	val source : InputStream = getClass.getResourceAsStream("/StopWords.txt")
	val stopWords : Array[String] = scala.io.Source.fromInputStream(source).mkString.split("\n")

	
	def classify(wordCounts:(String,Int), goodWords:Array[String], badWords:Array[String]) : (String, Int, String) = {
		if(goodWords.contains(wordCounts._1)) {
			return (wordCounts._1,wordCounts._2,"g")
		}
		else if(badWords.contains(wordCounts._1)) {
			return (wordCounts._1,wordCounts._2,"b")
		}
		else {
			return (wordCounts._1,wordCounts._2,"n")
		}
	}

	/*
	*  Takes a string and replaces any profanity or stop words.
	*/
	def replaceUnwantedWords(message:String) : String = {
		var result = message

		//Replace all stop words
		for (stopWord <- stopWords) {
			result = result.replaceAll("\\b" + stopWord + "\\b","")
		}
		//Replace all profanity words
		for (profanityWord <- profanityWords) {
			result = result.replaceAll("\\b" + profanityWord + "\\b","profanity")
		}
		//Remove any retweet tags
		result = result.replaceAll("\\brt\\b","")
		//Remove any URIs
		result = result.replaceAll("\\bhttp[^\\s]*\\b","")
		result = result.replaceAll("\\s+"," ")
		result = result.trim()
		return result
	}

	/*
	*  Put everything to lower case, alpha characters only.
	*/
	def standardiseString(message:String) : String = {
		var result = message

		result = result.toLowerCase()
		result = result.replaceAll("[^a-z\\s]","")

		return result
	}

	/*
	*  Takes an rdd of tuples (string, int, string) - classification,count,timestamp and outputs
	*  them to a given socket - IP & port
	*/
	
	def printMessageClassifications(rdd:RDD[(List[String], scala.collection.immutable.Iterable[(String, Int, String)])]) : String = {

		var jsonOut = "{\"name\": \"flare\",\"children\":["
		

		rdd.collect().foreach(item =>{
			jsonOut = jsonOut +  "{\"userName\":\"" + item._1(0) + 
							"\",\"activityID\":\"" + item._1(1) + 
							"\",\"startDate\":\"" + item._1(2) +
							"\",\"waitTime\":\"" + item._1(3) +
							"\",\"chatDuration\":\"" + item._1(4) +
							"\",\"abandon\":\"" + item._1(5)
			var countList = "\",\"wc\":["
			item._2.foreach(count => { 
			countList = countList + "{\"name\":\"" + count._1 +
							"\",\"size\":\"" + count._2 +
							"\",\"type\":\"" + count._3 +
							"\"},"})
			countList = countList.replaceAll(",$","")
			jsonOut = jsonOut + countList + "]},"
		
		})

		jsonOut = jsonOut.replaceAll(",$","")
		jsonOut = jsonOut + "]}\n"
		
		//jsonOut.saveAsTextFile("/home/Training/output/jsonout")
		//sc.parallelize(jsonOut).saveAsTextFile("/home/Training/output/jsonout")
		//println(jsonOut) 
		return jsonOut
	}

}