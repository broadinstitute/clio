package org.broadinstitute.clio.util.model

import scala.util.Random

/**
  * A unique identifier for each operation that modifies the database.
  */
final class UpsertId private (val id: String) extends Ordered[UpsertId] {
  override def compare(that: UpsertId): Int = id.compareTo(that.id)
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: UpsertId => id.equals(that.id)
      case _              => false
    }
  }
  override def hashCode(): Int = id.hashCode
  override def toString: String = id

  /**
    * The filename used for persisting a document's upsert data.
    *
    * @return the filename where the upsert with the given ID's data is stored
    */
  def persistenceFilename: String = s"$id.json"
}

/**
  * Generate a lexically-ordered, unique ID based on the Firebase Push ID.
  *
  * Each ID is a 20-byte string of by 8 characters of timestamp
  * followed by 12 characters of random (yet sequential) data.
  *
  * A recent one looks like this: -KuzbQJIFBhwvtkvrBHF
  *
  * @see [[https://firebase.googleblog.com/2015/02/the-2120-ways-to-ensure-unique_68.html]]
  */
object UpsertId {

  private val source = new Random()

  /**
    * Map from 7-bit data to character encoding.
    */
  private val Encoding =
    "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"

  /**
    * There are this many characters of timestamp data.
    */
  private val TimestampCount = 8

  /**
    * There are this many characters of randomized data.
    */
  private val RandomCount = 12

  /**
    * The length of a valid upsert ID.
    */
  val IdLength: Int = TimestampCount + RandomCount

  /**
    * The latest time used to construct an ID.
    */
  private var was = 0L

  /**
    * 8 characters encoded from `was`.
    */
  private var timestamp = "timestam"

  /**
    * 12 characters of monotonic increment seeded with a random value.
    */
  private var randomCharacters = Array.fill[Int](12)(Encoding.length)

  /**
    * Return a random index into `Encoding`.
    */
  private val next6bitIndex = (_: Int) => source.nextInt(Encoding.length)

  /**
    * Return with new values for was, timestamp, and randomCharacters.
    *
    * @param now is the current system time
    */
  private def refreshRandomState(now: Long): Unit = {
    was = now
    timestamp = Stream
      .iterate(now, TimestampCount)(_ / Encoding.length)
      .map(n => Encoding((n % Encoding.length).toInt))
      .reverse
      .mkString
    randomCharacters =
      Stream.iterate(next6bitIndex(0), RandomCount)(next6bitIndex(_)).toArray
  }

  /**
    * Increment `randomCharacters` or wait until `now` changes when
    * `randomCharacters` would roll over to 0.
    */
  private def incrementRandomBytes(now: Long): Unit = {
    val where = randomCharacters.lastIndexWhere(_ != Encoding.length - 1)
    val randomRollover = where == -1
    if (randomRollover) {
      var still = now
      while (still == was) {
        still = System.currentTimeMillis()
        refreshRandomState(now)
      }
    } else {
      randomCharacters(where) = randomCharacters(where) + 1
      for (n <- where + 1 until RandomCount) randomCharacters(n) = 0
    }
  }

  /**
    * Check if `idText` is a valid upsert ID.
    *
    * @param idText the text to validate
    * @return true if `idText` is a valid ID
    */
  def isValidId(idText: String): Boolean = {
    idText.length == IdLength && idText.forall(Encoding.contains(_))
  }

  /**
    * Create an ID from `idText`. The text must be a valid ID.
    *
    * @param idText the text to construct an ID from
    * @return the created ID or `None`
    */
  def fromString(idText: String): Option[UpsertId] = {
    List(idText).filter(isValidId).map(new UpsertId(_)).headOption
  }

  /**
    * Return the next unique ID.
    *
    * @return a 20-character unique ID
    */
  val nextId: () => UpsertId = () =>
    synchronized {
      val now = System.currentTimeMillis()
      if (now == was) {
        incrementRandomBytes(now)
      } else {
        refreshRandomState(now)
      }
      new UpsertId(timestamp + randomCharacters.map(Encoding(_)).mkString)
  }
}
