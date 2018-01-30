package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.{HitReader, Indexable}
import org.broadinstitute.clio.util.generic.FieldMapper

import scala.reflect.{ClassTag, classTag}

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
  lazy val rootDir: String = indexName.replaceAll("_", "-") + "/"

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

  /**
    * Creates an index using shapeless and reflection.
    *
    * @param fieldMapper The object defining how the Scala types of the fields in `Document` should be translated
    *                    into Elasticsearch field types.
    * @tparam Document The type of the Elasticsearch document, with a context bound also specifying that both an
    *                  `implicit ctag: ClassTag[Document]` exists, plus an
    *                  `implicit fieldMapper: FieldMapper[Document]` exists.
    *                  https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @return The index.
    */
  private[elasticsearch] def apply[
    Document <: ClioDocument: ClassTag: FieldMapper: Indexable: HitReader
  ](fieldMapper: ElasticsearchFieldMapper): ElasticsearchIndex[Document] = {
    val esName =
      ElasticsearchUtil
        .toElasticsearchName(classTag[Document].runtimeClass.getSimpleName)
        .stripPrefix("document_")

    // We started without a version suffix, so we keep it that way to avoid breaking things.
    val versionSuffix = if (fieldMapper.value == 1) "" else s"_v${fieldMapper.value}"

    new ElasticsearchIndex[Document](s"$esName$versionSuffix", fieldMapper)
  }

  /** Implicit summoner, for convenience. */
  def apply[Document: ElasticsearchIndex]: ElasticsearchIndex[Document] =
    implicitly[ElasticsearchIndex[Document]]

  implicit val WgsUbam: ElasticsearchIndex[DocumentWgsUbam] =
    ElasticsearchIndex(ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords)

  implicit val Gvcf: ElasticsearchIndex[DocumentGvcf] =
    ElasticsearchIndex(ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords)

  implicit val WgsCram: ElasticsearchIndex[DocumentWgsCram] =
    ElasticsearchIndex(ElasticsearchFieldMapper.StringsToTextFieldsWithSubKeywords)
}
