package net.walend.enron

/**
 * Parse the CSVs to an Iteratable[List[String]]
 *
 * @author dwalend
 * @since v0.0.0
 */

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.Source

object CsvParser {


  //todo try rewrite with @tailreq
  def parseLine(line:Int,input:String):Either[Problem,Message] = {

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
          if (builder.size != 0) Left(Problem(line,s"Doublequote does not follow a comma at $column in $input"))
        }
        else if(char == ',') {
          strings += builder.toString()
          builder.clear
          expectComma = false
        }
        else if(expectComma) Left(Problem(line,s"Expected a comma at $column in $input"))
        else builder += char
      }
      else {
        if(char == '"') quoted = false //todo the next one should be a comma
        else builder += char
      }

    }
    if(quoted) Left(Problem(line,s"Unclosed double quote in $input"))
    else Message.create(line,strings)
  }

}

case class Problem(line:Int,description:String)

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

object ReadFile {
  def main (args: Array[String]):Unit = {

    //skip the first line -- column headers
    val lines:Iterable[String] = Source.fromFile("testdata/metadata1999.csv").getLines().toIterable.drop(1)

    //start the line numbers at 2.
    val results:Iterable[Either[Problem,Message]] = lines.zipWithIndex.map(x => (x._1,x._2+2)).map(x => CsvParser.parseLine(x._2,x._1))

    println(results.take(10).to[List].mkString("\n"))
  }
}
