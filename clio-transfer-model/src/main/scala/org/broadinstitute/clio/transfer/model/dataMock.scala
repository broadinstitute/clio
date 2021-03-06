package org.broadinstitute.clio.transfer.model

import java.net.URI
import java.time.OffsetDateTime

import cats.Show
import io.circe.{Decoder, Encoder}
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.reflect.ClassTag

case class ModelMockIndex(
  name: String = "mock",
  commandName: String = "mock",
  urlSegment: String = "mock"
) extends ClioIndex
    with ModelAutoDerivation {

  override type KeyType = ModelMockKey
  override type MetadataType = ModelMockMetadata
  override type QueryInputType = ModelMockQueryInput

  override val keyTag: ClassTag[KeyType] = implicitly[ClassTag[KeyType]]
  override val metadataTag: ClassTag[MetadataType] = implicitly[ClassTag[MetadataType]]
  override val queryInputTag: ClassTag[QueryInputType] =
    implicitly[ClassTag[QueryInputType]]
  override val keyEncoder: Encoder.AsObject[KeyType] =
    implicitly[Encoder.AsObject[KeyType]]
  override val keyDecoder: Decoder[KeyType] = implicitly[Decoder[KeyType]]
  override val metadataDecoder: Decoder[MetadataType] = implicitly[Decoder[MetadataType]]
  override val metadataEncoder: Encoder[MetadataType] = implicitly[Encoder[MetadataType]]
  override val queryInputEncoder: Encoder[QueryInputType] =
    implicitly[Encoder[QueryInputType]]
  override val queryInputDecoder: Decoder[ModelMockQueryInput] =
    implicitly[Decoder[QueryInputType]]
  override val keyMapper: FieldMapper[KeyType] = implicitly[FieldMapper[KeyType]]
  override val metadataMapper: FieldMapper[MetadataType] =
    implicitly[FieldMapper[MetadataType]]
  override val queryInputMapper: FieldMapper[ModelMockQueryInput] =
    implicitly[FieldMapper[QueryInputType]]

  override val showKey: Show[ModelMockKey] = _.toString
}

case class ModelMockKey(mockKeyLong: Long, mockKeyString: String) extends IndexKey {
  override val location = Location.OnPrem
  override def getUrlSegments = Seq(mockKeyLong.toString, mockKeyString)
}

case class ModelMockMetadata(
  mockFieldDouble: Option[Double] = None,
  mockFieldInt: Option[Int] = None,
  mockFieldDate: Option[OffsetDateTime] = None,
  mockStringArray: Option[Seq[String]] = None,
  mockPathArray: Option[Seq[URI]] = None,
  mockDocumentStatus: Option[DocumentStatus] = None,
  mockFileMd5: Option[Symbol] = None,
  mockFilePath: Option[URI] = None,
  mockFileSize: Option[Long] = None,
  mockDefaultField: Option[String] = None,
  mockEmptyField: Option[String] = None
) extends Metadata[ModelMockMetadata] {
  override val documentStatus: Option[DocumentStatus] = mockDocumentStatus
  override val notes: Option[String] = None
  override def pathsToDelete: Seq[URI] = Seq.empty[URI]
  override def changeStatus(
    documentStatus: DocumentStatus,
    markExternalNote: String
  ): ModelMockMetadata = this

  def withDocumentStatus(documentStatus: Option[DocumentStatus]): ModelMockMetadata =
    this.copy(mockDocumentStatus = documentStatus)
}

case class ModelMockQueryInput(
  mockFieldDouble: Option[Double] = None,
  mockFieldDateEnd: Option[OffsetDateTime] = None,
  mockFieldDateStart: Option[OffsetDateTime] = None,
  mockFieldInt: Option[Int] = None,
  mockKeyLong: Option[Long] = None,
  mockKeyString: Option[String] = None,
  documentStatus: Option[DocumentStatus] = None
) extends QueryInput[ModelMockQueryInput] {

  def withDocumentStatus(documentStatus: Option[DocumentStatus]): ModelMockQueryInput =
    this.copy(documentStatus = documentStatus)
}
