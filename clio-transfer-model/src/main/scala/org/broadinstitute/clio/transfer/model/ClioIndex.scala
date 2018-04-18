package org.broadinstitute.clio.transfer.model

import io.circe.{Decoder, Encoder, Json, ObjectEncoder}
import org.broadinstitute.clio.util.generic.FieldMapper

import scala.reflect.ClassTag

/**
  * This models an index in our database. An index is a table (schema) within our database.
  */
trait ClioIndex {

  /**
    * Encode the index as a url segment for making web service API calls
    *
    * @return the url path representation of this index
    */
  val urlSegment: String

  val name: String

  val commandName: String

  val elasticsearchIndexName: String

  type KeyType <: IndexKey

  type MetadataType <: Metadata[MetadataType]

  type QueryInputType <: QueryInput[QueryInputType]

  val keyTag: ClassTag[KeyType]

  val metadataTag: ClassTag[MetadataType]

  val queryInputTag: ClassTag[QueryInputType]

  val keyEncoder: ObjectEncoder[KeyType]

  val metadataDecoder: Decoder[MetadataType]

  val metadataEncoder: Encoder[MetadataType]

  val queryInputEncoder: Encoder[QueryInputType]

  val queryInputDecoder: Decoder[QueryInputType]

  val keyMapper: FieldMapper[KeyType]

  val metadataMapper: FieldMapper[MetadataType]

  val queryInputMapper: FieldMapper[QueryInputType]

  /**
    * Container for all index parameters that are typically
    * used as implicit arguments.
    *
    * Enables using `import index.implicits._` in other code
    * to pull in the context needed for generically working
    * with a specific index.
    */
  object implicits {
    implicit val kt: ClassTag[KeyType] = keyTag
    implicit val mt: ClassTag[MetadataType] = metadataTag
    implicit val qit: ClassTag[QueryInputType] = queryInputTag
    implicit val ke: ObjectEncoder[KeyType] = keyEncoder
    implicit val md: Decoder[MetadataType] = metadataDecoder
    implicit val me: Encoder[MetadataType] = metadataEncoder
    implicit val qie: Encoder[QueryInputType] = queryInputEncoder
    implicit val qid: Decoder[QueryInputType] = queryInputDecoder
    implicit val qim: FieldMapper[QueryInputType] = queryInputMapper
    implicit val km: FieldMapper[KeyType] = keyMapper
    implicit val mm: FieldMapper[MetadataType] = metadataMapper
  }
}
