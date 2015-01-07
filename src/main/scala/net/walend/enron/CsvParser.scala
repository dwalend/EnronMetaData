package net.walend.enron

/**
 * Parse the CSVs to an Iteratable[List[String]]
 *
 * @author dwalend
 * @since v0.0.0
 */

import java.nio.file.{Paths, Files}

import scala.collection.parallel.immutable.ParSeq
import scala.io.Source

object CsvParser {

  def parseLine(fileName:String,line:Int,input:String):Either[Problem,Transmission] = {

    //Scan through the string, looking for commas or double quotes. Build up the String in a StringBuffer.
    val strings = scala.collection.mutable.Buffer.empty[String]

    var quoted = false
    var expectComma = false
    var builder = new StringBuilder
    var column = 0

    for(char:Char <- input) {
      column = column + 1
      if(!quoted){
        if(char == '"') {
          quoted = true
          if (builder.size != 0) Left(Problem(fileName,line,Category.partialQuote.name,s"column $column in $input"))
        }
        else if(char == ',') {
          strings += builder.toString()
          builder.clear()
          expectComma = false
        }
        else if(expectComma) Left(Problem(fileName,line,Category.expectedCommaMissing.name,s"column $column in $input"))
        else builder += char
      }
      else {
        if(char == '"') quoted = false //todo the next one should be a comma
        else builder += char
      }

    }
    if(quoted) Left(Problem(fileName,line,Category.unclosedDoubleQuote.name,s"Unclosed double quote in $input"))
    else Transmission.create(fileName,line,strings)
  }

  def debugParseLine(fileName:String,line:Int,input:String):Either[Problem,Transmission] = {
    val result = parseLine(fileName,line,input)

//    if(result.isLeft) println(result.left)

    result
  }

  def linesToMessages(fileName:String,lines:Iterable[String]):Iterable[Either[Problem,Transmission]] = {
    println(s"Started $fileName")
    //start the line numbers at 2.
    val results = lines.zipWithIndex.map(x => (x._1,x._2+2)).map(x => CsvParser.debugParseLine(fileName,x._2,x._1))

//    val problems = results.flatMap(_.left.toOption)

//    println(problems.mkString("\n"))

    def possibleRelationshipsFilter(result:Either[Problem,Transmission]):Boolean = {
      if(result.isLeft) true
      else result.right.get.possibleRelationship
    }
    println(s"Finsihed $fileName")

    results.filter(possibleRelationshipsFilter)
  }
}

object ReadFiles {
  import java.io.File

  def main (args: Array[String]):Unit = {

    EnronDatabase.clearAndCreateTables

    val files:Seq[File] = filesInDir("testdata")
//    val files:Seq[File] = filesInDir("data/metadatatime").to[Seq]

//    val messages:Iterable[Either[Problem,Transmission]] = files.map(readFile).flatten
//    EnronDatabase.manyMessagesToDatabase(messages,1000)
    files.par.map(fileToDb)

    EnronDatabase.spewProblems()

//    EnronDatabase.spewTransmissions()

//    println(results.take(10).to[List].mkString("\n"))
//    val problems = messages.flatMap(_.left.toOption)
//    println(s"problem lines ${problems.size}")
//    println(problems.mkString("\n"))


/*
    val usableMessages:Iterable[Transmission] = messages.flatMap(_.right.toOption)

    val senderReceivers:Iterable[(Email,Email)] = usableMessages.map(x => (x.sender,x.recipient))

    //todo first group by send time within an interval

    val edges = usableMessages.groupBy(x => (x.sender,x.recipient)).map(x => (x._1,x._2.size)).toList.sortBy(_._2)

    //val edges:Set[(Email,Email)] = senderReceivers.to[Set]

    Files.write(Paths.get("results/edges.txt"),edges.mkString("\n").getBytes())

    println(s"edges ${edges.size}")

    val nodes:Set[Email] = (edges.map(x => x._1._1) ++ edges.map(x => x._1._2)).to[Set]
    //val nodes:Set[Email] = edges.map(x => x._1) ++ edges.map(x => x._2)
    println(s"nodes ${nodes.size}")
*/

  }

  def filesInDir(dirName:String):Seq[File] = {
    new File(dirName).listFiles()
  }

  def readFile(file:File):Iterable[Either[Problem,Transmission]] = {

    //skip the first line -- column headers
    val lines:Iterable[String] = Source.fromFile(file,"iso-8859-1").getLines().toIterable.drop(1)

    CsvParser.linesToMessages(file.toString,lines)
  }

  def fileToDb(file:File):Unit = {
    val messages = readFile(file)
    EnronDatabase.manyMessagesToDatabase(messages,1000)
  }

}
