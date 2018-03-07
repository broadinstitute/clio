package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.keywordField
import io.circe.Json
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchUtil.toElasticsearchName
import org.broadinstitute.clio.transfer.model.TransferIndex
import org.broadinstitute.clio.transfer.model.{GvcfIndex, WgsCramIndex, WgsUbamIndex}
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

/**
  * An index for an Elasticsearch document.
  *
  * @param fieldMapper The version of the mapping used to generate the index.
  * @tparam Index The TransferIndex of the document.
  */
class ElasticsearchIndex[Index <: TransferIndex](
  val index: Index,
  private[elasticsearch] val fieldMapper: ElasticsearchFieldMapper
) extends ModelAutoDerivation {
  import index.implicits._

  /**
    * The root directory to use when persisting updates of this index to storage.
    *
    * NOTE: It's required that this end with '/'. On local disk it's a meaningless
    * addition, but in GCS's filesystem adapter it's the only indication that this
    * should be treated as a directory, not a file.
    */
  lazy val rootDir: String = index.elasticsearchIndexName + "/"

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

  final val indexName: String = index.elasticsearchIndexName

  /**
    * The name of the index type. Always default until ES 7 when there will be no index types.
    * https://www.elastic.co/blog/elasticsearch-6-0-0-alpha1-released#type-removal
    */
  final val indexType: String = "default"

  /** The fields for the index. */
  def fields: Seq[FieldDefinition] =
    (FieldMapper[index.KeyType].fields ++ FieldMapper[index.MetadataType].fields).toSeq.sortBy {
      case (name, _) => name
    }.map({
      case (name, value) =>
        fieldMapper.stringToDefinition(value)(
          ElasticsearchUtil.toElasticsearchName(name)
        )
    }) ++ Seq(
      keywordField(ElasticsearchIndex.UpsertIdElasticsearchName),
      keywordField(ElasticsearchIndex.EntityIdElasticsearchName)
    )
}

object ElasticsearchIndex extends ModelAutoDerivation {
  val EntityIdElasticsearchName = "entity_id"

  val UpsertIdElasticsearchName = "upsert_id"

  def getEntityId(json: Json): String =
    json.hcursor
      .get[String](toElasticsearchName(EntityIdElasticsearchName))
      .fold(throw _, identity)

  def getUpsertId(json: Json): UpsertId =
    json.hcursor
      .get[UpsertId](toElasticsearchName(UpsertIdElasticsearchName))
      .fold(throw _, identity)

  /** Format the directory path for the indexed meta-data files. */
  lazy val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy/MM/dd")

  implicit val WgsUbam: ElasticsearchIndex[WgsUbamIndex.type] =
    new ElasticsearchIndex(
      WgsUbamIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  implicit val Gvcf: ElasticsearchIndex[GvcfIndex.type] =
    // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
    // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
    new ElasticsearchIndex(
      GvcfIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  implicit val WgsCram: ElasticsearchIndex[WgsCramIndex.type] =
    // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
    // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
    new ElasticsearchIndex(
      WgsCramIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
}
