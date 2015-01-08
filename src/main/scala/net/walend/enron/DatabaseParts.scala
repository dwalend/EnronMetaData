package net.walend.enron

//import scala.slick.driver.H2Driver.simple._
import scala.slick.driver.JdbcDriver.simple._
import scala.slick.jdbc.meta.MTable

import scala.slick.lifted.ProvenShape
import scala.slick.lifted.Query

/**
 *
 *
 * @author dwalend
 * @since v0.0.0
 */
case class Problem(fileName:String,line:Int,category:String,description:String)

sealed case class Category(name:String)

object Category {
  val partialQuote = Category("Doublequote does not follow a comma")
  val expectedCommaMissing = Category("Expected a comma")
  val unclosedDoubleQuote = Category("Unclosed double quote")
  val tooManyColumns = Category("Too many columns")
  val tooFewColumns = Category("Too few columns")
  val missingAt = Category("does not contain a @")
}

class Problems(tag:Tag) extends Table[Problem](tag,"problems") {
  def fileName = column[String]("fileName")
  def line = column[Int]("line")
  def category = column[String]("category")
  def description = column[String]("description")
  def * = (fileName,line,category,description) <> (Problem.tupled,Problem.unapply)
}

object Problems {
  lazy val table = TableQuery[Problems]
}

case class Email(address:String)

case class Transmission(fileName:String,
                        lineNumber:Int,
                        sender:Email,
                        recipient:Email,
                        subject:String,
                        messageURL:String,
                        dateLine:String,
                        toLine:String,
                        ccLine:String) {

  lazy val totalRecipients = toLine.count(_ == '@') + ccLine.count(_ == '@')

  def possibleRelationship = totalRecipients < 12
}

object Transmission extends ((String,Int,Email,Email,String,String,String,String,String) => Transmission) {
  def create(fileName:String,lineNumber:Int,lineContents:Seq[String]):Either[Problem,Transmission] = {
    if (lineContents.size != 11) {
      if (lineContents.size < 11) Left(Problem(fileName, lineNumber,Category.tooFewColumns.name, s"(${lineContents.size}) in $lineContents"))
      else Left(Problem(fileName, lineNumber,Category.tooManyColumns.name, s"(${lineContents.size}) in $lineContents"))
    }
    else {
      val sender = Email(lineContents(1))
      val recipient = Email(lineContents(2))
      if(!sender.address.contains('@')) Left(Problem(fileName,lineNumber,Category.missingAt.name,s"sender $sender"))
      else if (!recipient.address.contains('@')) Left(Problem(fileName,lineNumber,Category.missingAt.name,s"recipient $recipient"))
      else  Right(Transmission(fileName = fileName,
        lineNumber = lineNumber,
        sender = sender,
        recipient = recipient,
        subject = lineContents(3),
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
//  def dateTimeCode = column[Long]("time")
  def sender = column[String]("sender")
  def recipient = column[String]("recipient")
  def subject = column[String]("subject")
//  def isTo = column[Boolean]("isTo")
//  def isCC = column[Boolean]("isCC")
//  def isBCC = column[Boolean]("isBCC")
  def messageURL = column[String]("messageURL")
  def dateLine = column[String]("dateLine")
  def toLine = column[String]("toLine",O.DBType("VARCHAR(512)"))
  def ccLine = column[String]("ccLine",O.DBType("VARCHAR(512)"))
  def totalRecipients = column[Int]("totalRecipients")
  def * :ProvenShape[Transmission] = (fileName,line,sender,recipient,subject,messageURL,dateLine,toLine,ccLine,totalRecipients) <> (fromRow,toRow)

  def fromRow = (fromParams _).tupled

  def fromParams(fileName:String,
                  line:Int,
                  sender:String,
                  recipient:String,
                  subject:String,
                  messageURL:String,
                  dateLine:String,
                  toLine:String,
                  ccLine:String,
                  totalRecipients:Int):Transmission = {
    Transmission(fileName,line,Email(sender),Email(recipient),subject,messageURL,dateLine,toLine,ccLine)
  }

  def toRow(t:Transmission) = {
    Some((t.fileName,t.lineNumber,t.sender.address,t.recipient.address,t.subject,t.messageURL,t.dateLine,t.toLine,t.ccLine,t.totalRecipients))
  }
}

object Transmissions {
  lazy val table = TableQuery[Transmissions]
}

object EnronDatabase {

  val workingDirectory = System.getProperty("user.dir")
  val url = "jdbc:oracle:thin:@oracletestdb.cqma6h8xasfp.us-east-1.rds.amazonaws.com:1521:ORCL"

//  val database = Database.forURL("jdbc:h2:mem:enron", driver = "org.h2.Driver")
//val database = Database.forURL(s"jdbc:h2:$workingDirectory/results/enron.h2:enron", driver = "org.h2.Driver")
  lazy val database = Database.forURL(s"jdbc:oracle:thin:@oracletestdb.cqma6h8xasfp.us-east-1.rds.amazonaws.com:1521:ORCL", driver = "oracle.jdbc.driver.OracleDriver", user = "test", password = "yodler42")

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

  def testDbUrl(url:String) {
    import java.sql.DriverManager
    import java.util.Properties

    val props = new Properties()
    props.put("user","test")
    props.put("password","yodler42")

    DriverManager.getConnection(url,props)
  }

/*
  def query[Problem](query:Query[Problems,Problem,Seq]):Seq[Problem] = {
    database.withTransaction { implicit session =>
      Problems.table.
    }
  }
*/
}