package org.broadinstitute.clio.server.dataaccess.util

import org.broadinstitute.clio.util.model.UpsertId
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class UpsertIdGeneratorSpec extends FlatSpec with Matchers {
  behavior of "UpsertIdGenerator"

  /**
    * Generate this many IDs.
    */
  private val Count = 100000

  /**
    * Wrap ID generation in a future to test thread-safety somewhat.
    *
    * @return a future sequence of IDs
    */
  def generateIds(): Future[Seq[UpsertId]] = Future.sequence {
    Seq.fill(Count) {
      Future(UpsertIdGenerator.nextId())
    }
  }

  it should "generate unique 20-character IDs" in {
    generateIds().map { ids =>
      ids
        .filter(id => id.id.length == 20)
        .distinct should have length Count.toLong
    }
  }

  it should "generate IDs in ascending order" in {
    val fids = generateIds()
    fids.map { ids =>
      ids.sortWith(_.compareTo(_) < 0) should be(ids)
    }
  }
}
