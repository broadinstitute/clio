package org.broadinstitute.clio.util.model

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class UpsertIdSpec extends FlatSpec with Matchers {
  behavior of "UpsertId"

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
      Future(UpsertId.nextId())
    }
  }

  it should s"generate unique ${UpsertId.IdLength}-character IDs" in {
    generateIds().map { ids =>
      ids
        .filter(id => id.id.length == UpsertId.IdLength)
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
