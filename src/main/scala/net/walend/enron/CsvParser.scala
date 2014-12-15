package net.walend.enron

/**
 * Parse the CSVs to an Iteratable[List[String]]
 *
 * @author dwalend
 * @since v0.0.0
 */

import scala.collection.mutable
import scala.io.Source

object CsvParser {

  def parseLine(fileName:String,line:Int,input:String):Either[Problem,Message] = {

    //Scan through the string, looking for commas or double quotes. Build up the String in a StringBuffer.
    val strings = mutable.Buffer.empty[String]

    var quoted = false
    var expectComma = false
    var builder = new StringBuilder
    var column = 0

    for(char:Char <- input) {
      column = column + 1
      if(!quoted){
        if(char == '"') {
          quoted = true
          if (builder.size != 0) Left(Problem(fileName,line,s"Doublequote does not follow a comma at $column in $input"))
        }
        else if(char == ',') {
          strings += builder.toString()
          builder.clear()
          expectComma = false
        }
        else if(expectComma) Left(Problem(fileName,line,s"Expected a comma at $column in $input"))
        else builder += char
      }
      else {
        if(char == '"') quoted = false //todo the next one should be a comma
        else builder += char
      }

    }
    if(quoted) Left(Problem(fileName,line,s"Unclosed double quote in $input"))
    else Message.create(line,strings)
  }

  def linesToMessages(fileName:String,lines:Iterable[String]):Iterable[Either[Problem,Message]] = {
    //start the line numbers at 2.
    lines.zipWithIndex.map(x => (x._1,x._2+2)).map(x => CsvParser.parseLine(fileName,x._2,x._1))
  }
}

case class Problem(fileName:String,line:Int,description:String)

case class Email(address:String)

case class Message(lineNumber:Int,
                   dateTimeCode:Long,
                   sender:Email,
                   recipient:Email,
                   subject:String,
                   isTo:Boolean,
                   isCC:Boolean,
                   isBCC:Boolean,
                   messageURL:String,
                   dateLine:String,
                   toLine:String,
                   ccLine:String) {
}

object Message {
  def create(lineNumber:Int,lineContents:Seq[String]):Either[Problem,Message] = {
    Right(Message(lineNumber = lineNumber,
      dateTimeCode = java.lang.Long.parseLong(lineContents(0))*1000,
      sender = Email(lineContents(1)),
      recipient = Email(lineContents(2)),
      subject = lineContents(3),
      isTo = java.lang.Boolean.parseBoolean(lineContents(4)),
      isCC = java.lang.Boolean.parseBoolean(lineContents(5)),
      isBCC = java.lang.Boolean.parseBoolean(lineContents(6)),
      messageURL = lineContents(7),
      dateLine = lineContents(8),
      toLine = lineContents(9),
      ccLine = lineContents(10)
    ))

  }
}

object ReadFiles {
  import java.io.File

  def main (args: Array[String]):Unit = {

//    val files:Seq[File] = filesInDir("testdata")
    val files:Seq[File] = filesInDir("data/metadatatime")

    val messages:Iterable[Either[Problem,Message]] = files.map(readFile).flatten

//    println(results.take(10).to[List].mkString("\n"))
    val problems = messages.filter(_.isLeft)
    println(problems.size)

    val usableMessages:Iterable[Message] = messages.flatMap(_.right.toOption)

    //todo first group by send time within an interval

    //todo start here. (sender,recipient,count) triplets next.
    val counts = usableMessages.groupBy(x => (x.sender,x.recipient)).map(x => (x._1,x._2.size)).toList.sortBy(_._2)

    println(counts.mkString("\n"))
  }

  def filesInDir(dirName:String):Seq[File] = {
    new File(dirName).listFiles()
  }

  def readFile(file:File):Iterable[Either[Problem,Message]] = {

    //skip the first line -- column headers
    val lines:Iterable[String] = Source.fromFile(file).getLines().toIterable.drop(1)

    CsvParser.linesToMessages(file.toString,lines)
  }
}
