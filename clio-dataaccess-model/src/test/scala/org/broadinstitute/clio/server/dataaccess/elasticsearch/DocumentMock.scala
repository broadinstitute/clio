package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.net.URI
import java.time.OffsetDateTime

import org.broadinstitute.clio.util.model.UpsertId

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
  mockPathArray: Option[Seq[URI]] = None
) extends ClioDocument

object DocumentMock extends Elastic4sAutoDerivation {
  import com.sksamuel.elastic4s.circe._

  def default: DocumentMock = DocumentMock(
    upsertId = UpsertId.nextId(),
    entityId = Symbol("1234.the-string"),
    mockKeyLong = 1234L,
    mockKeyString = "the-string",
    mockFileMd5 = Some(Symbol("12345abcdefg")),
    mockFilePath = Some(URI.create("gs://the-bucket/the-path.file")),
    mockStringArray = Some(Seq.empty),
    mockPathArray = Some(Seq.empty)
  )

  implicit val index: ElasticsearchIndex[DocumentMock] =
    ElasticsearchIndex[DocumentMock](
      ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
    )

  val indexWithTextFields: ElasticsearchIndex[DocumentMock] =
    ElasticsearchIndex[DocumentMock](
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
}
