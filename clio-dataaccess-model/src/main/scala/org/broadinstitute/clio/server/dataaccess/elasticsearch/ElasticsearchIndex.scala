package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.{HitReader, Indexable}
import org.broadinstitute.clio.util.generic.FieldMapper

/**
  * An index for an Elasticsearch document.
  *
  * @param indexName The name of the index in Elasticsearch.
  * @param fieldMapper The version of the mapping used to generate the index.
  * @param indexable Typeclass used to convert `Document` instances into JSON.
  * @param hitReader Typeclass used to convert ES search hits back to `Document` instances.
  * @tparam Document The type of the Elasticsearch document.
  */
class ElasticsearchIndex[Document: FieldMapper](
  val indexName: String,
  private[elasticsearch] val fieldMapper: ElasticsearchFieldMapper
)(
  implicit
  private[clio] val indexable: Indexable[Document],
  private[clio] val hitReader: HitReader[Document]
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
    * should be persisted now.
    */
  def currentPersistenceDir: String = {
    val now = OffsetDateTime.now().format(ElasticsearchIndex.dateTimeFormatter)
    s"$rootDir$now/"
  }

  /**
    * The name of the index type. Always default until ES 7 when there will be no index types.
    * https://www.elastic.co/blog/elasticsearch-6-0-0-alpha1-released#type-removal
    */
  final val indexType: String = "default"

  /** The fields for the index. */
  def fields: Seq[FieldDefinition] =
    FieldMapper[Document].fields.toSeq.sortBy {
      case (name, _) => name
    } map {
      case (name, value) =>
        fieldMapper.stringToDefinition(value)(
          ElasticsearchUtil.toElasticsearchName(name)
        )
    }
}

object ElasticsearchIndex extends Elastic4sAutoDerivation {
  import com.sksamuel.elastic4s.circe._

  /** Format the directory path for the indexed meta-data files. */
  lazy val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy/MM/dd")

  /** Implicit summoner, for convenience. */
  def apply[Document: ElasticsearchIndex]: ElasticsearchIndex[Document] =
    implicitly[ElasticsearchIndex[Document]]

  implicit val WgsUbam: ElasticsearchIndex[DocumentWgsUbam] =
    new ElasticsearchIndex[DocumentWgsUbam](
      "wgs-ubam",
      ElasticsearchFieldMapper.NumericBooleanDateAndKeywordFields
    )

  implicit val Gvcf: ElasticsearchIndex[DocumentGvcf] =
    /* Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
     * Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
     */
    new ElasticsearchIndex[DocumentGvcf](
      "gvcf-v2",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )

  implicit val WgsCram: ElasticsearchIndex[DocumentWgsCram] =
    /* Despite being decoupled from "v1", we append -v2 to keep ES indices consistent with GCS.
     * Since we compute GCS paths from the ES index name, inconsistency would break GCS paths.
     */
    new ElasticsearchIndex[DocumentWgsCram](
      "wgs-cram-v2",
      ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords
    )
}
