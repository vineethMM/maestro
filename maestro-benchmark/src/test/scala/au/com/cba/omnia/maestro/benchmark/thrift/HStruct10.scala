package au.com.cba.omnia.maestro.benchmark.thrift

import shapeless._

import au.com.cba.omnia.maestro.core.codec.Decode

class HStruct10(args: String :: String :: Double :: Int :: Long :: String :: String :: Double :: Int :: Long :: HNil) extends Product{
  val str1: String = args.head
  def _1: String = str1

  val str2: String = args.tail.head
  def _2: String = str2

  val dbl3: Double = args.tail.tail.head
  def _3: Double = dbl3

  val int4: Int = args.tail.tail.tail.head
  def _4: Int = int4

  val long5: Long = args.tail.tail.tail.tail.head
  def _5: Long = long5

  val str6: String = args.tail.tail.tail.tail.tail.head
  def _6: String = str6

  val str7: String = args.tail.tail.tail.tail.tail.tail.head
  def _7: String = str7

  val dbl8: Double = args.tail.tail.tail.tail.tail.tail.tail.head
  def _8: Double = dbl8

  val int9: Int = args.tail.tail.tail.tail.tail.tail.tail.tail.head
  def _9: Int = int9

  val long10: Long = args.tail.tail.tail.tail.tail.tail.tail.tail.tail.head
  def _10: Long = long10

  def productArity: Int = 10

  def productElement(n: Int): Any = n match {
    case 0 => _1
    case 1 => _2
    case 2 => _3
    case 3 => _4
    case 4 => _5
    case 5 => _6
    case 6 => _7
    case 7 => _8
    case 8 => _9
    case 9 => _10
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def equals(other: Any) = other match {
    case null => false
    case that: HStruct10 =>
      List(this.str1, this.str2, this.dbl3, this.int4, this.long5, this.str6, this.str7, this.dbl8, this.int9, this.long10) == List(that.str1, that.str2, that.dbl3, that.int4, that.long5, that.str6, that.str7, that.dbl8, that.int9, that.long10)
    case _ => false
  }

  override def canEqual(other: Any) = other.isInstanceOf[HStruct10]

  override def hashCode(): Int = scala.util.hashing.MurmurHash3.productHash(this)

  override def toString: String = "HStruct10(" + List(this.str1, this.str2, this.dbl3, this.int4, this.long5, this.str6, this.str7, this.dbl8, this.int9, this.long10).mkString(", ") + ")"
}

object HStruct10 {
  implicit val HStruct10Decoder: Decode[HStruct10] = Decode(((none, source, position) => {
    import scala.util.control.NonFatal
    import scalaz.\&/.That
    import au.com.cba.omnia.maestro.core.codec.{DecodeOk, DecodeError, DecodeResult, ParseError, NotEnoughInput}
    if (source.length < 10)
      DecodeError[(List[String], Int, HStruct10)](source, position, NotEnoughInput(10, "au.com.cba.omnia.maestro.benchmark.thrift.HStruct10"))
    else
      {
        val fields = source.take(10).toArray
        var index = -1
        var tag = ""
        try {
          val result = new HStruct10(fields(0) :: fields(1) :: {
            tag = "Double"
            index = 2
            fields(index).toDouble
          } :: {
            tag = "Int"
            index = 3
            fields(index).toInt
          } :: {
            tag = "Long"
            index = 4
            fields(index).toLong
          } :: fields(5) :: fields(6) :: {
            tag = "Double"
            index = 7
            fields(index).toDouble
          } :: {
            tag = "Int"
            index = 8
            fields(index).toInt
          } :: {
            tag = "Long"
            index = 9
            fields(index).toLong
          } :: HNil)
          DecodeOk((source.drop(10), position.$plus(10), result))
        } catch {
          case NonFatal((e @ _)) => DecodeError[(List[String], Int, HStruct10)](source.drop(index - position), position + index, ParseError(fields(index), tag, That(e)))
        }
      }
  }))

}
