package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.keywordField
import io.circe.{Decoder, Json}
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.transfer.model.{GvcfIndex, WgsCramIndex, WgsUbamIndex}
import org.broadinstitute.clio.util.generic.FieldMapper
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId

/**
  * An index for an Elasticsearch document.
  *
  * @param fieldMapper The version of the mapping used to generate the index.
  * @tparam CI The ClioIndex of the document.
  */
class ElasticsearchIndex[CI <: ClioIndex](
  val clioIndex: CI,
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
  lazy val rootDir: String = clioIndex.elasticsearchIndexName + "/"

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

  final val indexName: String = clioIndex.elasticsearchIndexName

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
  val EntityIdElasticsearchName = "entity_id"

  val UpsertIdElasticsearchName = "upsert_id"

  val BookkeepingNames = Seq(
    UpsertIdElasticsearchName,
    EntityIdElasticsearchName
  )

  def getEntityId(json: Json): String =
    getByName[String](json, EntityIdElasticsearchName)

  def getUpsertId(json: Json): UpsertId = {
    getByName[UpsertId](json, UpsertIdElasticsearchName)
  }

  def getByName[A: Decoder](json: Json, name: String): A = {
    json.hcursor.get[A](name).fold(throw _, identity)
  }

  /** Format the directory path for the indexed meta-data files. */
  lazy val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy/MM/dd")

  implicit val WgsUbam: ElasticsearchIndex[WgsUbamIndex.type] =
    new ElasticsearchIndex(
      WgsUbamIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  implicit val Gvcf: ElasticsearchIndex[GvcfIndex.type] =
    new ElasticsearchIndex(
      GvcfIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  implicit val WgsCram: ElasticsearchIndex[WgsCramIndex.type] =
    new ElasticsearchIndex(
      WgsCramIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
}
