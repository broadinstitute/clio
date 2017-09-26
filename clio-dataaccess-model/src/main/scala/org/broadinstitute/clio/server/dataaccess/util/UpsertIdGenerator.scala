package org.broadinstitute.clio.server.dataaccess.util

import scala.util.Random

/**
  * Generate a lexically-ordered, unique ID based on the Firebase Push ID.
  *
  * A Push ID is a 20-byte string of by 8 characters of timestamp
  * followed by 12 characters of randomized (yet sequential) data.
  *
  * A recent one looks like this: -KuzbQJIFBhwvtkvrBHF
  *
  * @see [[https://firebase.googleblog.com/2015/02/the-2120-ways-to-ensure-unique_68.html]]
  *
  */
object UpsertIdGenerator {

  private val push =
    "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"

  private val source = new Random()

  private val timestampCount = 8
  private val randomCount = 12

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
  private var randomCharacters = Array.fill[Int](12)(push.length)

  /**
    * Return a random index into `push`.
    */
  private val next7bitIndex = (_: Int) => source.nextInt(push.length)

  /**
    * Return with new values for was, timestamp, and randomCharacters.
    *
    * @param now is the current system time
    */
  private def refreshRandomState(now: Long): Unit = {
    was = now
    timestamp = Stream
      .iterate(now, timestampCount)(_ / push.length)
      .map(n => push((n % push.length).toInt))
      .reverse
      .mkString
    randomCharacters =
      Stream.iterate(next7bitIndex(0), randomCount)(next7bitIndex(_)).toArray
  }

  /**
    * Increment `randomCharacters` or wait until `now` changes when
    * `randomCharacters` would roll over to 0.
    */
  private def incrementRandomBytes(now: Long): Unit = {
    val where = randomCharacters.lastIndexWhere(_ != push.length - 1)
    val randomRollover = where == -1
    if (randomRollover) {
      var still = now
      while (still == was) {
        still = System.currentTimeMillis()
        refreshRandomState(now)
      }
    } else {
      randomCharacters(where) = randomCharacters(where) + 1
      for (n <- where + 1 until randomCount) randomCharacters(n) = 0
    }
  }

  /**
    * Return the next unique ID.
    *
    * @return a 20-character unique ID string
    */
  val nextId: () => String = () =>
    synchronized {
      val now = System.currentTimeMillis()
      if (now == was) {
        incrementRandomBytes(now)
      } else {
        refreshRandomState(now)
      }
      timestamp + randomCharacters.map(push(_)).mkString
  }
}
