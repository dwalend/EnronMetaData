package net.walend.enron

/**
 * Parse the CSVs to a List[List[String]]
 *
 * shamelessly started as a cut-paste from https://github.com/sirthias/parboiled2/blob/master/examples/src/main/scala/org/parboiled2/examples/CsvParser.scala
 *
 * @author dwalend
 * @since v0.0.0
 */
import scala.collection.immutable
import org.parboiled2._

object CsvParser extends {

  case class CsvFile(header: Option[Record], records: immutable.Seq[Record])
  case class Record(fields: immutable.Seq[String])
  case class Error(msg: String)

  /**
   * Parses the given input into a [[CsvFile]] or an [[Error]] instance.
   */
  def apply(input: ParserInput, headerPresent: Boolean = true, fieldDelimiter: Char = ','): Either[Error, CsvFile] = {
    import Parser.DeliveryScheme.Either
    val parser = new CsvParser(input, headerPresent, fieldDelimiter)
    parser.file.run().left.map(error => Error(parser.formatError(error)))
  }

  private val `TEXTDATA-BASE` = CharPredicate.Printable -- '"'
  private val QTEXTDATA = `TEXTDATA-BASE` ++ "\r\n"
}

/**
 * Simple, fast CSV parser.
 *
 * See http://tools.ietf.org/html/rfc4180#section-2
 */
class CsvParser(val input: ParserInput, headerPresent: Boolean, fieldDelimiter: Char) extends Parser with StringBuilding {
  import CsvParser._

  val TEXTDATA = `TEXTDATA-BASE` -- fieldDelimiter

  def file = rule {
    OWS ~ optional(test(headerPresent) ~ header ~ NL) ~ oneOrMore(record).separatedBy(NL) ~ optional(NL) ~ EOI ~> CsvFile
  }

  def header = rule { record }

  def record = rule { oneOrMore(field).separatedBy(fieldDelimiter ~ OWS) ~> Record }

  def field = rule { `quoted-field` | `unquoted-field` }

  def `quoted-field` = rule {
    '"' ~ clearSB() ~ zeroOrMore((QTEXTDATA | '"' ~ '"') ~ appendSB()) ~ '"' ~ OWS ~ push(sb.toString)
  }

  def `unquoted-field` = rule { capture(zeroOrMore(TEXTDATA)) }

  def NL = rule { optional('\r') ~ '\n' ~ OWS }

  def OWS = rule { zeroOrMore(' ') }
}