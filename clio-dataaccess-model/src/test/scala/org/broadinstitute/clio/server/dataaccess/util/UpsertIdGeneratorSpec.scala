package org.broadinstitute.clio.server.dataaccess.util

import org.scalatest.{FlatSpec, Matchers}

class UpsertIdGeneratorSpec extends FlatSpec with Matchers {
  behavior of "UpsertIdGenerator"

  val count = 100000
  def generateIds(): Seq[String] = Seq.fill(count) {
    UpsertIdGenerator.nextId()
  }

  it should "generate unique 20-character IDs" in {
    generateIds()
      .filter(id => id.length == 20)
      .distinct should have length count.toLong
  }

  it should "generate IDs in ascending order" in {
    val ids = generateIds()
    ids.sortWith(_.compareTo(_) < 0) should be(ids)
  }
}
