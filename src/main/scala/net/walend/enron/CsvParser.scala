package net.walend.enron

/**
 * Parse the CSVs to an Iteratable[List[String]]
 *
 * @author dwalend
 * @since v0.0.0
 */

import scala.annotation.tailrec

object CsvParser {


//todo not working. Just rip the line  @tailrec
  def parseStrings(line:Int,string:String,index:Int):List[String] = {

    //Scan through the string, looking for commas or double quotes, build up a substring in a StringBuilder
    val nextComma = string.indexOf(',',index)
//    val nextDoubleQuote = string.indexOf('"',index)
    val nextStart = nextComma + 1

    if (nextComma == -1) List(string.substring(index))
    else (string.substring(index, nextComma))::parseStrings(line, string, nextStart)
  }

  def parseLine(line:Int,string:String):Either[Problem,List[String]] = {

    //Scan through the string, looking for commas or double quotes, build up a substring in a StringBuilder

    //When you find a comma, add the string to the existing Seq[String], then keep looking (recursive?)

    //When you find a double quote, change modes until you find the closing quote. (Problem if you don't find it.)
    Right(parseStrings(line,string,0))
  }


}

case class Problem(line:Int,description:String)


object ReadFile {
  def main (args: Array[String]):Unit = {
    import java.nio.file.{Files, Paths}

    val byteArray = Files.readAllBytes(Paths.get("testdata/metadata2000q1.csv"))

  }
}
