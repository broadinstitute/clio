package org.broadinstitute.clio.server.dataaccess.util

import org.scalatest.{FlatSpec, Matchers}

import java.util.UUID

class ClioUUIDGeneratorSpec extends FlatSpec with Matchers {
  behavior of "ClioUUIDGenerator"

  val count = 100000
  def generateIds(): Seq[UUID] = Seq.fill(count) { ClioUUIDGenerator.getUUID() }

  it should "generate unique IDs" in {
    generateIds().distinct should have length count.toLong
  }

  it should "generate IDs in ascending order" in {
    val ids = generateIds()
    ids.sortWith(_.compareTo(_) < 0) should be(ids)
  }
}
