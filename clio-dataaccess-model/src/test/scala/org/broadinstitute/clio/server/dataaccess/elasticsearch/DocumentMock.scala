package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DocumentStatus, UpsertId}

import scala.util.Random

case class DocumentMock(
  upsertId: UpsertId,
  entityId: Symbol,
  mockKeyLong: Long,
  mockKeyString: String,
  mockFieldDate: Option[OffsetDateTime] = None,
  mockFieldDouble: Option[Double] = None,
  mockFieldInt: Option[Int] = None,
  mockFileMd5: Option[Symbol] = None,
  mockFilePath: Option[URI] = None,
  mockFileSize: Option[Long] = None,
  mockStringArray: Option[Seq[String]] = None,
  mockPathArray: Option[Seq[URI]] = None,
  mockDocumentStatus: Option[DocumentStatus] = None
) extends ClioDocument

object DocumentMock extends ModelAutoDerivation {

  def default: DocumentMock = {
    val long = Random.nextLong()
    val string = Random.nextString(10)
    DocumentMock(
      upsertId = UpsertId.nextId(),
      entityId = Symbol(s"$long.$string"),
      mockKeyLong = long,
      mockKeyString = string,
      mockFileMd5 = Some(Symbol("12345abcdefg")),
      mockFilePath = Some(URI.create("gs://the-bucket/the-path.file")),
      mockStringArray = Some(Seq.empty),
      mockPathArray = Some(Seq.empty),
      mockDocumentStatus = Some(DocumentStatus.Normal)
    )
  }

  implicit val index: ElasticsearchIndex[DocumentMock] =
    new ElasticsearchIndex[DocumentMock](
      "mock",
      ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
    )

  val indexWithTextFields: ElasticsearchIndex[DocumentMock] =
    new ElasticsearchIndex[DocumentMock](
      "mock",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
}
