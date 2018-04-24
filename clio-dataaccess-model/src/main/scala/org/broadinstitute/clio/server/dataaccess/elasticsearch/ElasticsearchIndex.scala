package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.keywordField
import io.circe.Json
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.extras.Configuration.snakeCaseTransformation
import org.broadinstitute.clio.transfer.model.ClioIndex
import org.broadinstitute.clio.transfer.model._
import org.broadinstitute.clio.transfer.model.arrays.ArraysMetadata
import org.broadinstitute.clio.transfer.model.gvcf.GvcfMetadata
import org.broadinstitute.clio.transfer.model.ubam.UbamMetadata
import org.broadinstitute.clio.transfer.model.wgscram.WgsCramMetadata
import org.broadinstitute.clio.util.generic.FieldMapper
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
  private[elasticsearch] val fieldMapper: ElasticsearchFieldMapper,
  private[elasticsearch] val defaultFields: Json = Json.obj()
) extends ModelAutoDerivation {
  import clioIndex.implicits._
  import ElasticsearchIndex._

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

  /**
    * The ID of a json record in elasticsearch. By default, each record's ID consists
    * of the concatenated values of the Key fields.
    * Additional fields can be added to the ID using the ElasticsearchIndex constructor.
    */
  def getId(json: Json): String = {
    val keyFields = FieldMapper[clioIndex.KeyType].fields.keys
      .map(snakeCaseTransformation)
    (keyFields ++ defaults.asObject.map(_.keys).getOrElse(Iterable.empty)).toSeq.sorted
      .map(json.getAsString)
      .mkString(".")
  }

  final val indexName: String = clioIndex.elasticsearchIndexName

  /**
    * The name of the index type. Always default until ES 7 when there will be no index types.
    * https://www.elastic.co/blog/elasticsearch-6-0-0-alpha1-released#type-removal
    */
  final val indexType: String = "default"

  final val defaults: Json = parse(defaultFields.pretty(implicitly)).getOrElse(Json.obj())

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
    EntityIdElasticsearchName,
    UpsertIdElasticsearchName
  )

  def getUpsertId(json: Json): UpsertId =
    json.unsafeGet[UpsertId](UpsertIdElasticsearchName)

  implicit class IdJson(val json: Json) extends AnyVal {

    def getAsString(key: String): String = {
      json.asObject
        .flatMap(_.apply(key))
        .flatMap {
          case s if s.isString =>
            s.asString
          case a if a.isArray || a.isObject =>
            throw new RuntimeException("Arrays and objects cannot be used as ID fields")
          case j => Option(j.toString())
        }
        .getOrElse(throw new RuntimeException(s"Could not get $key from json"))
    }
  }

  /** Format the directory path for the indexed meta-data files. */
  lazy val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy/MM/dd")

  val WgsUbam: ElasticsearchIndex[WgsUbamIndex.type] =
    new ElasticsearchIndex(
      WgsUbamIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords,
      UbamMetadata().asJson
    )

  val Gvcf: ElasticsearchIndex[GvcfIndex.type] =
    new ElasticsearchIndex(
      GvcfIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords,
      GvcfMetadata(dataType = Option(DataType.WGS)).asJson
    )

  val WgsCram: ElasticsearchIndex[WgsCramIndex.type] =
    new ElasticsearchIndex(
      WgsCramIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords,
      WgsCramMetadata(dataType = Option(DataType.WGS)).asJson
    )

  val Arrays: ElasticsearchIndex[ArraysIndex.type] =
    new ElasticsearchIndex(
      ArraysIndex,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords,
      ArraysMetadata().asJson
    )
}
