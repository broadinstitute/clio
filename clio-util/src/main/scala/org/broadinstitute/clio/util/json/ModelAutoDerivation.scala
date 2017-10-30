package org.broadinstitute.clio.util.json

import java.net.URI
import java.time.OffsetDateTime

import cats.syntax.either._
import enumeratum._
import io.circe.generic.extras.{AutoDerivation, Configuration}
import io.circe._
import org.broadinstitute.clio.util.generic.CompanionCache
import org.broadinstitute.clio.util.model.UpsertId

import scala.reflect.ClassTag

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
    Configuration.default.withSnakeCaseKeys

  /**
    * By default, compact JSON as much as possible by removing spaces and keys with null / None values.
    */
  implicit val defaultPrinter: Printer =
    Printer.noSpaces.copy(dropNullKeys = true)

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
    Encoder.encodeString contramap [OffsetDateTime] { offsetDateTime =>
      offsetDateTime.toString
    }
  }

  implicit val decodeOffsetDateTime: Decoder[OffsetDateTime] = {
    Decoder.decodeString emap { string =>
      Either
        .catchNonFatal(OffsetDateTime.parse(string))
        .leftMap(_ => "OffsetDateTime")
    }
  }

  implicit val encodeUpsertId: Encoder[UpsertId] =
    Encoder.encodeString contramap [UpsertId] { _.id }

  implicit val decodeUpsertId: Decoder[UpsertId] = {
    Decoder.decodeString emap { string =>
      Either
        .fromOption(
          UpsertId.fromString(string),
          DecodingFailure(s"$string is not a valid upsert ID", List())
        )
        .leftMap(_ => "UpsertId")
    }
  }

  // Caches for enum encoders and decoders.
  private val enumEncoderCache = new CompanionCache
  private val enumDecoderCache = new CompanionCache

  /**
    * Provides an encoder for any class that extends EnumEntry.
    *
    * Using java reflection it retrieves the EnumEntry's companion, and builds a circe encoder using the library
    * enumeratum-circe. The encoder is then cached using the type T.
    *
    * @tparam T The type of the enum, with a context bound also specifying that an `implicit ctag: ClassTag[T]` exists.
    *           https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @return The enum encoder.
    */
  implicit def encodeEnum[T <: EnumEntry: ClassTag]: Encoder[T] = {
    enumEncoderCache.cached[T, Enum[T], Encoder[T]](Circe.encoder)
  }

  /**
    * Provides a decoder for any class that extends EnumEntry.
    *
    * Using java reflection it retrieves the EnumEntry's companion, and builds a circe decoder using the library
    * enumeratum-circe. The decoder is then cached using the type T.
    *
    * @tparam T The type of the enum, with a context bound also specifying that an `implicit ctag: ClassTag[T]` exists.
    *           https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @return The enum decoder.
    */
  implicit def decodeEnum[T <: EnumEntry: ClassTag]: Decoder[T] = {
    enumDecoderCache.cached[T, Enum[T], Decoder[T]](Circe.decoder)
  }

  implicit val encodeUri: Encoder[URI] =
    Encoder.encodeString.contramap(_.toString)

  implicit val decodeUri: Decoder[URI] = {
    Decoder.decodeString.emap { string =>
      Either
        .catchNonFatal(URI.create(string))
        .leftMap(_ => "URI")
    }
  }

  implicit val encodeSymbol: Encoder[Symbol] =
    Encoder.encodeString.contramap(_.name)

  implicit val decodeSymbol: Decoder[Symbol] =
    Decoder.decodeString.map(Symbol.apply)
}

/**
  * Companion object so we can `import ModelAutoDerivation._` to get the implicits within when
  * that is more convenient than extending the trait.
  */
object ModelAutoDerivation extends ModelAutoDerivation
