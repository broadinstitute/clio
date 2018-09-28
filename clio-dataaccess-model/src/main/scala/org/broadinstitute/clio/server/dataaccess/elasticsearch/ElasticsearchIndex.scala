package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.keywordField
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.clio.JsonUtils.JsonOps
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.{DataType, UpsertId}

/**
  * An index for an Elasticsearch document.
  *
  * @param fieldMapper The version of the mapping used to generate the index.
  * @tparam CI The ClioIndex of the document.
  */
class ElasticsearchIndex[CI <: ClioIndex](
  val clioIndex: CI,
  final val indexName: String,
  private[elasticsearch] val fieldMapper: ElasticsearchFieldMapper,
  private[elasticsearch] val defaultFields: Json = Json.obj()
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

  private val keyFields = fieldMapper.mapFields[clioIndex.KeyType]
  private val metadataFields = fieldMapper.mapFields[clioIndex.MetadataType]

  /**
    * The ID of a json record in elasticsearch. By default, each record's ID consists
    * of the concatenated values of the Key fields.
    * Additional fields can be added to the ID using the ElasticsearchIndex constructor.
    */
  def getId(json: Json): String = {

    def getAsString(key: String): String = {
      json.asObject
        .flatMap(_.apply(key))
        .flatMap {
          case s if s.isString =>
            s.asString
          case a if a.isArray || a.isObject =>
            throw new RuntimeException(
              s"""Found '$a' for key $key
                 |with upsertId ${ElasticsearchIndex.getUpsertId(json)}
                 |in index $indexName.
                 |Arrays and objects cannot be used as ID fields""".stripMargin
            )
          case j => Some(j.toString())
        }
        .getOrElse(throw new RuntimeException(s"Could not get $key from json"))
    }

    keyFields.map(field => getAsString(field.name)).mkString(".")
  }

  /**
    * The name of the index type. Always default until ES 7 when there will be no index types.
    * https://www.elastic.co/blog/elasticsearch-6-0-0-alpha1-released#type-removal
    */
  final val indexType: String = "default"

  final val defaults: Json = defaultFields.dropNulls

  /** The fields for the index. */
  def fields: Seq[FieldDefinition] =
    Seq
      .concat(
        keyFields,
        metadataFields,
        ElasticsearchIndex.BookkeepingNames.map(keywordField)
      )
      .sortBy(_.name)
}

object ElasticsearchIndex extends ModelAutoDerivation {
  import org.broadinstitute.clio.JsonUtils.JsonOps

  val EntityIdElasticsearchName = "entity_id"

  val UpsertIdElasticsearchName = "upsert_id"

  val dataTypeKey = "data_type"

  val BookkeepingNames = Seq(
    EntityIdElasticsearchName,
    UpsertIdElasticsearchName
  )

  def getUpsertId(json: Json): UpsertId =
    json.unsafeGet[UpsertId](UpsertIdElasticsearchName)

  /** Format the directory path for the indexed meta-data files. */
  lazy val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy/MM/dd")

  val Ubam: ElasticsearchIndex[UbamIndex.type] =
    new ElasticsearchIndex(
      UbamIndex,
      // We need to keep this name consistent with GCS, so we cannot easily change it.
      // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
      "wgs-ubam",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  val Gvcf: ElasticsearchIndex[GvcfIndex.type] =
    new ElasticsearchIndex(
      GvcfIndex,
      // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
      // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
      indexName = "gvcf-v2",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords,
      // Type-widening is needed here because `asJson` tries to jsonify the narrowest type possible.
      // This results in an empty object instead if the correct string value.
      Json.obj(dataTypeKey -> (DataType.WGS: DataType).asJson)
    )

  val Cram: ElasticsearchIndex[CramIndex.type] =
    new ElasticsearchIndex(
      CramIndex,
      // Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
      // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
      "wgs-cram-v2",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords,
      // Type-widening is needed here because `asJson` tries to jsonify the narrowest type possible.
      // This results in an empty object instead if the correct string value.
      Json.obj(dataTypeKey -> (DataType.WGS: DataType).asJson)
    )

  val Arrays: ElasticsearchIndex[ArraysIndex.type] =
    new ElasticsearchIndex(
      ArraysIndex,
      // We need to keep this name consistent with GCS, so we cannot easily change it.
      // Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
      "arrays",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
}
