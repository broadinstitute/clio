package org.broadinstitute.clio.server.dataaccess.elasticsearch

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.{HitReader, Indexable}
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.broadinstitute.clio.util.generic.FieldMapper

import scala.reflect.{ClassTag, classTag}

/**
  * An index for an Elasticsearch document.
  *
  * @tparam Document The type of the Elasticsearch document.
  */
abstract class ElasticsearchIndex[Document] {

  /** The name of the index. */
  def indexName: String

  /** The version of the mappings used to generate the index. */
  def mappingsVersion: Int

  /** Typeclass used to convert `Document` instances into JSON. */
  def indexable: Indexable[Document]

  /** Typeclass used to convert ES search hits back to `Document` instances. */
  def hitReader: HitReader[Document]

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
  def fields: Seq[FieldDefinition]
}

object ElasticsearchIndex extends Elastic4sAutoDerivation {
  import com.sksamuel.elastic4s.circe._

  val WgsUbam: ElasticsearchIndex[DocumentWgsUbam] =
    indexDocument[DocumentWgsUbam](version = 1)

  val Gvcf: ElasticsearchIndex[DocumentGvcf] =
    indexDocument[DocumentGvcf](version = 2)

  val WgsCram: ElasticsearchIndex[DocumentWgsCram] =
    indexDocument[DocumentWgsCram](version = 2)

  /**
    * Name to assign to a nested keyword field under every text field,
    * to support exact matching.
    */
  val TextExactMatchFieldName: String = "exact"

  /**
    * Format the directory path for the indexed meta-data files.
    */
  lazy val dateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy/MM/dd")

  /**
    * Creates an index using shapeless and reflection.
    *
    * @tparam Document The type of the Elasticsearch document, with a context bound also specifying that both an
    *                  `implicit ctag: ClassTag[Document]` exists, plus an
    *                  `implicit fieldMapper: FieldMapper[Document]` exists.
    *                  https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @return The index.
    */
  private[dataaccess] def indexDocument[
    Document: ClassTag: FieldMapper: Indexable: HitReader
  ](version: Int): ElasticsearchIndex[Document] = {
    val esName =
      ElasticsearchUtil
        .toElasticsearchName(classTag[Document].runtimeClass.getSimpleName)
        .stripPrefix("document_")
    new AutoElasticsearchIndex[Document](esName, version)
  }
}
