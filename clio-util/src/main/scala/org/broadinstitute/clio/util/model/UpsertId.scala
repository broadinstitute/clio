package org.broadinstitute.clio.util.model

final case class UpsertId(id: String) extends Ordered[UpsertId] {
  override def compare(that: UpsertId): Int = id.compareTo(that.id)
  override def toString: String = id
}
