package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.keywordField
import io.circe.Json
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
  val defaults: Json = Json.obj(),
  private[elasticsearch] val fieldMapper: ElasticsearchFieldMapper,
  private[elasticsearch] val additionalIdFields: Seq[String] = Seq.empty
) extends ModelAutoDerivation {
  import clioIndex.implicits._
  import ElasticsearchUtil.JsonOps

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
    // Very hacky way of getting key fields without instantiating dummy keys
    val keyFields = clioIndex.keyTag.runtimeClass.getConstructors.head.getParameters
      .map(_.getName)
      .map(snakeCaseTransformation)
    (keyFields ++ additionalIdFields).map(json.unsafeGet[String]).mkString
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

  val WgsUbam: ElasticsearchIndex[WgsUbamIndex.type] =
    new ElasticsearchIndex(
      WgsUbamIndex,
      UbamMetadata().asJson,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  val Gvcf: ElasticsearchIndex[GvcfIndex.type] =
    new ElasticsearchIndex(
      GvcfIndex,
      GvcfMetadata(dataType = Option(DataType.WGS)).asJson,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  val WgsCram: ElasticsearchIndex[WgsCramIndex.type] =
    new ElasticsearchIndex(
      WgsCramIndex,
      WgsCramMetadata(dataType = Option(DataType.WGS)).asJson,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  val Arrays: ElasticsearchIndex[ArraysIndex.type] =
    new ElasticsearchIndex(
      ArraysIndex,
      ArraysMetadata().asJson,
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
}
