package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.keywordField
import io.circe.Json
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

/**
  * An index for an Elasticsearch document.
  *
  * @param fieldMapper The version of the mapping used to generate the index.
  * @tparam CI The ClioIndex of the document.
  */
class ElasticsearchIndex[+CI <: ClioIndex](
  val clioIndex: CI,
  final val indexName: String,
  private[elasticsearch] val fieldMapper: ElasticsearchFieldMapper
) extends ModelAutoDerivation {
  import clioIndex.implicits._

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
    (FieldMapper[clioIndex.KeyType].fields ++ FieldMapper[clioIndex.MetadataType].fields).toSeq.sortBy {
      case (name, _) => name
    }.map({
      case (name, value) =>
        fieldMapper.stringToDefinition(value)(
          ElasticsearchUtil.toElasticsearchName(name)
        )
    }) ++ ElasticsearchIndex.BookkeepingNames.map(keywordField)
}

object ElasticsearchIndex extends ModelAutoDerivation {
  import ElasticsearchUtil.JsonOps

  val EntityIdElasticsearchName = "entity_id"

  val UpsertIdElasticsearchName = "upsert_id"

  val BookkeepingNames = Seq(
    UpsertIdElasticsearchName,
    EntityIdElasticsearchName
  )

  def getEntityId(json: Json): String =
    json.unsafeGet[String](EntityIdElasticsearchName)

  def getUpsertId(json: Json): UpsertId =
    json.unsafeGet[UpsertId](UpsertIdElasticsearchName)

  /** Format the directory path for the indexed meta-data files. */
  lazy val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy/MM/dd")

  val Ubam: ElasticsearchIndex[UbamIndex.type] =
    new ElasticsearchIndex(
      UbamIndex,
      "wgs-ubam",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  val Gvcf: ElasticsearchIndex[GvcfIndex.type] =
    new ElasticsearchIndex(
      GvcfIndex,
      // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
      // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
      "gvcf-v2",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  val WgsCram: ElasticsearchIndex[WgsCramIndex.type] =
    new ElasticsearchIndex(
      WgsCramIndex,
      // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
      // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
      "wgs-cram-v2",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  val Arrays: ElasticsearchIndex[ArraysIndex.type] =
    new ElasticsearchIndex(
      ArraysIndex,
      "arrays",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
}
