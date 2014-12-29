package net.walend.enron

import scala.slick.driver.H2Driver.simple._
import scala.slick.jdbc.meta.MTable

import scala.slick.lifted.ProvenShape

/**
 *
 *
 * @author dwalend
 * @since v0.0.0
 */
case class Problem(fileName:String,line:Int,description:String)

class Problems(tag:Tag) extends Table[Problem](tag,"problems") {
  def fileName = column[String]("fileName")
  def line = column[Int]("line")
  def description = column[String]("description")
  def * = (fileName,line,description) <> (Problem.tupled,Problem.unapply)
}

object Problems {
  lazy val table = TableQuery[Problems]
}

case class Email(address:String)

case class Transmission(fileName:String,
                        lineNumber:Int,
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

  lazy val totalRecipients = toLine.count(_ == '@') + ccLine.count(_ == '@')

  def possibleRelationship = totalRecipients < 12
}

object Transmission extends ((String,Int,Long,Email,Email,String,Boolean,Boolean,Boolean,String,String,String,String) => Transmission) {
  def create(fileName:String,lineNumber:Int,lineContents:Seq[String]):Either[Problem,Transmission] = {
    if (lineContents.size != 11) {
      if (lineContents.size < 11) Left(Problem(fileName, lineNumber, s"Too few columns (${lineContents.size}) in $lineContents"))
      else Left(Problem(fileName, lineNumber, s"Too many columns (${lineContents.size}) in $lineContents"))
    }
    else {
      val sender = Email(lineContents(1))
      val recipient = Email(lineContents(2))
      if(!sender.address.contains('@')) Left(Problem(fileName,lineNumber,s"sender $sender does not contain a @"))
      else if (!recipient.address.contains('@')) Left(Problem(fileName,lineNumber,s"recipient $recipient does not contain a @"))
      else  Right(Transmission(fileName = fileName,
        lineNumber = lineNumber,
        dateTimeCode = java.lang.Long.parseLong(lineContents(0)) * 1000,
        sender = sender,
        recipient = recipient,
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
}

class Transmissions(tag:Tag) extends Table[Transmission](tag,"transmissions") {
  def fileName = column[String]("fileName")
  def line = column[Int]("line")
  def dateTimeCode = column[Long]("time")
  def sender = column[String]("sender")
  def recipient = column[String]("recipient")
  def subject = column[String]("subject")
  def isTo = column[Boolean]("isTo")
  def isCC = column[Boolean]("isCC")
  def isBCC = column[Boolean]("isBCC")
  def messageURL = column[String]("messageURL")
  def dateLine = column[String]("dateLine")
  def toLine = column[String]("toLine")
  def ccLine = column[String]("ccLine")
  def totalRecipients = column[Int]("totalRecipients")
  def * :ProvenShape[Transmission] = (fileName,line,dateTimeCode,sender,recipient,subject,isTo,isCC,isBCC,messageURL,dateLine,toLine,ccLine,totalRecipients) <> (fromRow,toRow)

  def fromRow = (fromParams _).tupled

  def fromParams(fileName:String,
                  line:Int,
                  dateTimeCode:Long,
                  sender:String,
                  recipient:String,
                  subject:String,
                  isTo:Boolean,
                  isCC:Boolean,
                  isBCC:Boolean,
                  messageURL:String,
                  dateLine:String,
                  toLine:String,
                  ccLine:String,
                  totalRecipients:Int):Transmission = {
    Transmission(fileName,line,dateTimeCode,Email(sender),Email(recipient),subject,isTo,isCC,isBCC,messageURL,dateLine,toLine,ccLine)
  }

  def toRow(t:Transmission) = {
    Some((t.fileName,t.lineNumber,t.dateTimeCode,t.sender.address,t.recipient.address,t.subject,t.isTo,t.isCC,t.isBCC,t.messageURL,t.dateLine,t.toLine,t.ccLine,t.totalRecipients))
  }
}

object Transmissions {
  lazy val table = TableQuery[Transmissions]
}

object EnronDatabase {

  val workingDirectory = System.getProperty("user.dir")
//  val database = Database.forURL("jdbc:h2:mem:enron", driver = "org.h2.Driver")
  val database = Database.forURL(s"jdbc:h2:$workingDirectory/results/enron.h2:enron", driver = "org.h2.Driver")

  def clearAndCreateTables = {
    database.withSession{ implicit session =>
      if (MTable.getTables("problems").list.nonEmpty) {
        Problems.table.ddl.drop
      }
      if (MTable.getTables("transmissions").list.nonEmpty) {
        Transmissions.table.ddl.drop
      }

      Problems.table.ddl.create
      Transmissions.table.ddl.create
    }
  }

  def messagesToDatabase(messages:Iterable[Either[Problem,Transmission]]):Unit = {

    database.withTransaction { implicit session =>
      messages.foreach(_.fold(Problems.table.insert, Transmissions.table.insert))
    }
  }

  def manyMessagesToDatabase(messages:Iterable[Either[Problem,Transmission]],blockSize:Int):Unit = {
    messages.grouped(blockSize).foreach(messagesToDatabase)
  }

  def spewProblems():Unit = {
    database.withTransaction { implicit session =>
      Problems.table.foreach{case x => println(s".$x")}
    }
  }

  def spewTransmissions():Unit = {
    database.withTransaction { implicit session =>
      Transmissions.table.foreach{case x => println(s".$x")}
    }
  }
}