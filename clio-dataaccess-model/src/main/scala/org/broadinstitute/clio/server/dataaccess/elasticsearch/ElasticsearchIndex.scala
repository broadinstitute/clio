package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.keywordField
import io.circe.{Decoder, Encoder}
import org.broadinstitute.clio.transfer.model.gvcf.{
  TransferGvcfV1Key,
  TransferGvcfV1Metadata
}
import org.broadinstitute.clio.transfer.model.ubam.{
  TransferUbamV1Key,
  TransferUbamV1Metadata
}
import org.broadinstitute.clio.transfer.model.wgscram.{
  TransferWgsCramV1Key,
  TransferWgsCramV1Metadata
}
import org.broadinstitute.clio.transfer.model.{TransferKey, TransferMetadata}
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{UpsertId, EntityId}

/**
  * An index for an Elasticsearch document.
  *
  * @param indexName The name of the index in Elasticsearch.
  * @param fieldMapper The version of the mapping used to generate the index.
  * @param encoder Typeclass used to convert `Key` and `Metadata` instances into JSON.
  * @param decoder Typeclass used to convert ES search hits back to `Key` and `Metadata` instances.
  * @tparam Key The TransferKey of the document.
  * @tparam Metadata The TransferMetadata of the document.
  */
class ElasticsearchIndex[Key <: TransferKey, Metadata <: TransferMetadata[Metadata]](
  val indexName: String,
  private[elasticsearch] val fieldMapper: ElasticsearchFieldMapper
)(
  implicit
  private[clio] val encoder: Encoder[(TransferKey, TransferMetadata[Metadata])],
  private[clio] val decoder: Decoder[(TransferKey, TransferMetadata[Metadata])]
) {

  /**
    * The root directory to use when persisting updates of this index to storage.
    *
    * NOTE: It's required that this end with '/'. On local disk it's a meaningless
    * addition, but in GCS's filesystem adapter it's the only indication that this
    * should be treated as a directory, not a file.
    */
  lazy val rootDir: String = indexName + "/"

  /**
    * The source-of-truth directory in which updates to this index
    * should be persisted, if applied now.
    */
  def currentPersistenceDir: String = persistenceDirForDatetime(OffsetDateTime.now())

  /**
    * The source-of-truth directory in which updates to this index
    * should be persisted, if applied at the given date-time.
    */
  def persistenceDirForDatetime(dt: OffsetDateTime): String = {
    val dir = dt.format(ElasticsearchIndex.dateTimeFormatter)
    s"$rootDir$dir/"
  }

  /**
    * The name of the index type. Always default until ES 7 when there will be no index types.
    * https://www.elastic.co/blog/elasticsearch-6-0-0-alpha1-released#type-removal
    */
  final val indexType: String = "default"

  /** The fields for the index. */
  def fields: Seq[FieldDefinition] =
    (FieldMapper[TransferMetadata[Metadata]].fields.toSeq.sortBy {
      case (name, _) => name
    } map {
      case (name, value) =>
        fieldMapper.stringToDefinition(value)(
          ElasticsearchUtil.toElasticsearchName(name)
        )
    }) ++ (FieldMapper[TransferKey].fields.toSeq.sortBy {
      case (name, _) => name
    } map {
      case (name, value) =>
        fieldMapper.stringToDefinition(value)(
          ElasticsearchUtil.toElasticsearchName(name)
        )
    }) ++ Seq(
      keywordField(ElasticsearchUtil.toElasticsearchName(UpsertId.UpsertIdFieldName)),
      keywordField(ElasticsearchUtil.toElasticsearchName(EntityId.EntityIdFieldName))
    )
}

object ElasticsearchIndex extends ModelAutoDerivation {

  /** Format the directory path for the indexed meta-data files. */
  lazy val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy/MM/dd")

  /** Implicit summoner, for convenience. */
  def apply[Key, Metadata]: ElasticsearchIndex[Key, Metadata] =
    implicitly[ElasticsearchIndex[Key, Metadata]]

  implicit val WgsUbam: ElasticsearchIndex[TransferUbamV1Key, TransferUbamV1Metadata] =
    new ElasticsearchIndex[TransferUbamV1Key, TransferUbamV1Metadata](
      "wgs-ubam",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  implicit val Gvcf: ElasticsearchIndex[TransferGvcfV1Key, TransferGvcfV1Metadata] =
    // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
    // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
    new ElasticsearchIndex[TransferGvcfV1Key, TransferGvcfV1Metadata](
      "gvcf-v2",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  implicit val WgsCram: ElasticsearchIndex[
    TransferWgsCramV1Key,
    TransferWgsCramV1Metadata
  ] =
    // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
    // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
    new ElasticsearchIndex[TransferWgsCramV1Key, TransferWgsCramV1Metadata](
      "wgs-cram-v2",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
}
