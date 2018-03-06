package org.broadinstitute.clio.transfer.model

import java.net.URI
import java.time.OffsetDateTime

import io.circe.{Decoder, Encoder, Json}
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}

import scala.reflect.ClassTag

case class ModelMockIndex(
  name: String = "mock",
  commandName: String = "mock",
  urlSegment: String = "mock",
  jsonSchema: Json = Json.Null,
) extends TransferIndex
    with ModelAutoDerivation {

  override type KeyType = ModelMockKey
  override type MetadataType = ModelMockMetadata
  override type QueryInputType = ModelMockQueryInput
  override type QueryOutputType = ModelMockQueryOutput

  override val keyTag: ClassTag[KeyType] = implicitly[ClassTag[KeyType]]
  override val metadataTag: ClassTag[MetadataType] = implicitly[ClassTag[MetadataType]]
  override val queryInputTag: ClassTag[QueryInputType] =
    implicitly[ClassTag[QueryInputType]]
  override val queryOutputTag: ClassTag[QueryOutputType] =
    implicitly[ClassTag[QueryOutputType]]
  override val keyEncoder: Encoder[KeyType] = implicitly[Encoder[KeyType]]
  override val metadataDecoder: Decoder[MetadataType] = implicitly[Decoder[MetadataType]]
  override val metadataEncoder: Encoder[MetadataType] = implicitly[Encoder[MetadataType]]
  override val queryInputEncoder: Encoder[QueryInputType] =
    implicitly[Encoder[QueryInputType]]
  override val queryOutputDecoder: Decoder[QueryOutputType] =
    implicitly[Decoder[QueryOutputType]]
  override val keyMapper: FieldMapper[KeyType] = implicitly[FieldMapper[KeyType]]
  override val metadataMapper: FieldMapper[MetadataType] =
    implicitly[FieldMapper[MetadataType]]
}

case class ModelMockKey(mockKeyLong: Long, mockKeyString: String) extends TransferKey {
  override val location = Location.OnPrem
  override def getUrlSegments = Seq.empty[String]
}

case class ModelMockMetadata(
  mockFieldDouble: Option[Double],
  mockFieldInt: Option[Int],
  mockFieldDate: Option[OffsetDateTime],
  mockStringArray: Option[Seq[String]] = None,
  mockPathArray: Option[Seq[URI]] = None,
  mockDocumentStatus: Option[DocumentStatus] = None,
  mockFileMd5: Option[Symbol] = None,
  mockFilePath: Option[URI] = None,
  mockFileSize: Option[Long] = None,
) extends TransferMetadata[ModelMockMetadata] {
  override val documentStatus: Option[DocumentStatus] = mockDocumentStatus
  override val notes: Option[String] = None
  override def pathsToDelete: Seq[URI] = Seq.empty[URI]
  override def markDeleted(deletionNote: String): ModelMockMetadata = this
  override protected def mapMove(
    pathMapper: (Option[URI], String) => Option[URI]
  ): ModelMockMetadata = this
}

case class ModelMockQueryInput(
  mockFieldDouble: Option[Double],
  mockFieldDateEnd: Option[OffsetDateTime],
  mockFieldDateStart: Option[OffsetDateTime],
  mockFieldInt: Option[Int],
  mockKeyLong: Option[Long],
  mockKeyString: Option[String]
)

case class ModelMockQueryOutput(
  mockKeyLong: Long,
  mockKeyString: String,
  mockFieldDouble: Option[Double],
  mockFieldInt: Option[Int],
  mockFieldDate: Option[OffsetDateTime],
  mockFileMd5: Option[Symbol],
  mockFilePath: Option[URI],
  mockFileSize: Option[Long],
  mockStringArray: Option[Seq[String]] = None,
  mockPathArray: Option[Seq[URI]] = None,
  mockDocumentStatus: Option[DocumentStatus] = None
)
