package org.broadinstitute.clio.util.json

import java.net.URI
import java.time.OffsetDateTime

import cats.syntax.either._
import enumeratum._
import io.circe.generic.extras.{AutoDerivation, Configuration}
import io.circe._
import io.circe.parser._
import org.broadinstitute.clio.util.model.UpsertId

/**
  * Extends circe's _extra_ auto derivation with additional derivations needed for clio.
  *
  * The _extra_ auto derivation is experimental, but adds configuration snake_case key name support.
  *
  * One could have a separate object that doesn't extend AutoDerivation, and then import the original
  * `io.circe.generic.extras.auto._` plus this object. BUT in that case, IntelliJ doesn't detect that this object is
  * needed for the enhanced auto derivation. It then marks this object's import as not used, and deletes in when the
  * "Optimize Imports" command is run.
  *
  * This is also idea because we can ensure instead of `io.circe.generic.extras.auto._` that classes are
  * encoding/decoding using only this derivation.
  */
trait ModelAutoDerivation extends AutoDerivation {

  /**
    * Adds "extra" snake case configuration by default.
    *
    * https://circe.github.io/circe/codec.html#custom-key-mappings-via-annotations
    * NOTE: We're extending from generic.extras.AutoDerivation instead of generic.AutoDerivation
    * Otherwise this config isn't used.
    */
  implicit val jsonConfig: Configuration =
    Configuration.default.withSnakeCaseMemberNames

  /**
    * By default, compact JSON as much as possible by removing spaces and keys with null / None values.
    */
  implicit val defaultPrinter: Printer =
    Printer.noSpaces.copy(dropNullValues = true)

  /**
    * When decoding, don't allow extra fields.
    *
    * @tparam T The type of decoder being created.
    * @return The decoder for type T.
    */
  implicit def decodeState[T: StateDecoder]: Decoder[T] = {
    Decoder.fromState(StateDecoder[T].state)
  }

  // https://circe.github.io/circe/codec.html#custom-encodersdecoders
  implicit val encodeOffsetDateTime: Encoder[OffsetDateTime] = {
    Encoder.encodeString.contramap(_.toString)
  }

  implicit val decodeOffsetDateTime: Decoder[OffsetDateTime] = {
    Decoder.decodeString emap { string =>
      Either
        .catchNonFatal(OffsetDateTime.parse(string))
        .leftMap(ex => s"$string is not a valid OffsetDateTime: ${ex.getMessage}")
    }
  }

  implicit val encodeUpsertId: Encoder[UpsertId] =
    Encoder.encodeString.contramap(_.id)

  implicit val decodeUpsertId: Decoder[UpsertId] = {
    Decoder.decodeString emap { string =>
      Either.fromOption(
        UpsertId.fromString(string),
        s"$string is not a valid upsert ID"
      )
    }
  }

  /**
    * Provides an encoder for any class that extends EnumEntry.
    *
    * @tparam T The specific type of the enum.
    * @param enum The companion `Enum` which knows all possible values of `T`.
    *             This should always be in scope for any given subtype of `EnumEntry`
    *             thanks to this macro:
    *             https://github.com/lloydmeta/enumeratum/blob/master/enumeratum-core/src/main/scala/enumeratum/Enum.scala#L173
    * @return The enum encoder.
    */
  implicit def encodeEnum[T <: EnumEntry](implicit enum: Enum[T]): Encoder[T] =
    Circe.encoder(enum)

  /**
    * Provides a decoder for any class that extends EnumEntry.
    *
    * @tparam T The specific type of the enum.
    * @param enum The companion `Enum` which knows all possible values of `T`.
    *             This should always be in scope for any given subtype of `EnumEntry`
    *             thanks to this macro:
    *             https://github.com/lloydmeta/enumeratum/blob/master/enumeratum-core/src/main/scala/enumeratum/Enum.scala#L173
    * @return The enum decoder.
    */
  implicit def decodeEnum[T <: EnumEntry](implicit enum: Enum[T]): Decoder[T] =
    Circe.decoder(enum)

  implicit val encodeUri: Encoder[URI] =
    Encoder.encodeString.contramap(_.toString)

  implicit val decodeUri: Decoder[URI] = {
    Decoder.decodeString.emap { string =>
      Either
        .catchNonFatal(URI.create(string))
        .leftMap(ex => s"$string is not a valid URI: ${ex.getMessage}")
    }
  }

  implicit val encodeSymbol: Encoder[Symbol] =
    Encoder.encodeString.contramap(_.name)

  implicit val decodeSymbol: Decoder[Symbol] =
    Decoder.decodeString.map(Symbol.apply)

  def dropNullsFromJson(json: Json): Json = {
    parse(json.pretty(implicitly)).getOrElse(Json.obj())
  }
}

/**
  * Companion object so we can `import ModelAutoDerivation._` to get the implicits within when
  * that is more convenient than extending the trait.
  */
object ModelAutoDerivation extends ModelAutoDerivation
