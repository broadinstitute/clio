package org.broadinstitute.clio.util.json

import cats.data.StateT
import cats.instances.either._
import io.circe.Decoder.Result
import io.circe.generic.extras.Configuration
import io.circe.{ACursor, Decoder}
import shapeless._
import shapeless.labelled._

/**
  * A stateful decoder for some type A.
  *
  * Based on the example from [[https://github.com/circe/circe/pull/375]].
  * Contains additional patches in `hconsOptionDecoder` such that `Option` fields do not cause decoding errors.
  *
  * Also based on examples from [[https://github.com/underscoreio/shapeless-guide Shapeless]].
  *
  * @tparam A The type decoded by state.
  *
  * @see [[org.broadinstitute.clio.util.generic.FieldMapper]]
  */
trait StateDecoder[A] {
  def state: StateT[Result, ACursor, A]
}

/**
  * Provides implicit StateDecoder instances that may be used with circe's `StateDecoder.fromState()`.
  *
  * For example, using the summoner `apply()` method:
  * ```
  * case class Foo(a: Option[String], b: Int)
  *
  * val stateDecoder = StateDecoder[Foo]
  * val decoder = Decoder.fromState(stateDecoder.state)
  * ```
  *
  * @see [[org.broadinstitute.clio.util.generic.FieldMapper]]
  */
object StateDecoder extends LowPriorityStateDecoder {

  /**
    * Summon any stateDecoder that implements StateDecoder[A].
    *
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/generic/type-classes.md#idiomatic-type-class-definitions-secgenericidiomatic-style
    */
  def apply[A](implicit stateDecoder: StateDecoder[A]): StateDecoder[A] =
    stateDecoder

  /**
    * A decoder for HNil.
    *
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/generic/products.md#instances-for-hlists
    *
    * Returns an error if the cursor isn't empty.
    */
  implicit val hnilDecoder: StateDecoder[HNil] = {
    createDecoder {
      Decoder.state.requireEmpty.map(_ => HNil)
    }
  }

  /**
    * A version of decodeField[A] that doesn't move the cursor when an Option is missing.
    * Don't move the cursor when options are missing.
    *
    * https://github.com/circe/circe/blob/v0.8.0/modules/core/shared/src/main/scala/io/circe/Decoder.scala#L819-L829
    *
    * @param k The key being extracted from the cursor.
    * @param d The decoder for the type within the option.
    * @tparam A The type within the option being decoded.
    * @return A StateT for decoding an Option[A].
    */
  private def decodeFieldOption[A](k: String)(
    implicit d: Decoder[A]
  ): StateT[Result, ACursor, Option[A]] = StateT[Result, ACursor, Option[A]] { c =>
    val field = c.downField(k)

    field.as[Option[A]] match {
      // Found the field. Delete it, and return the modified cursor.
      case Right(a @ Some(_)) => Right((field.delete, a))
      // The field was not found. Return the original cursor.
      case Right(None) => Right((c, None))
      // A decoding error occurred.
      case l @ Left(_) => l.asInstanceOf[Result[(ACursor, Option[A])]]
    }
  }

  /**
    * Retrieves a decoder for any HList made up of a (headNameAndOptionType :: (more tail elements) :: HNil).
    *
    * See [[LowPriorityStateDecoder.hlistDecoder]] for more details. This is the
    * equivalent, but instead of returning an decoder for `H`, returns a decoder for `Option[H]`.
    *
    * @param witness       Reads the symbol name.
    * @param tailDecoder   An implicit decoder for the tail. Either this method recursively, or just the hnilDecoder.
    * @param configuration Custom extra configuration for the circe decoder, possibly snake_casing the names.
    * @tparam K The singleton-type of the head. When used with the witness allows access to the filed name.
    * @tparam H The type of the head, with a context bound also specifying that an `implicit ev: Decoder[H]` exists.
    *           https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @tparam T The tail of the list, some list of headNameAndType :: tail.
    * @return A decoder than can decode the head-plus-tail.
    */
  implicit def hlistOptionDecoder[K <: Symbol, H: Decoder, T <: HList](
    implicit
    witness: Witness.Aux[K],
    tailDecoder: StateDecoder[T],
    configuration: Configuration = Configuration.default
  ): StateDecoder[FieldType[K, Option[H]] :: T] = {
    val symbol = witness.value
    val name = configuration.transformKeys(symbol.name)
    createDecoder {
      for {
        head <- decodeFieldOption[H](name)
        tail <- tailDecoder.state
      } yield field[K](head) :: tail
    }
  }

  /**
    * Creates a decoder for any type that can be converted to an HList representation. The HList must be a labeled
    * version so that the names may be retrieved.
    *
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/representations/products.md#generic-product-encodings
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/labelled-generic/literal-types.md#type-tagging-and-phantom-types-seclabelled-generictype-tagging
    *
    * @param gen Creates an instance of A from a generic representation ARepr.
    * @param decoder Decodes an A.
    * @tparam A The original type.
    * @tparam ARepr The shapeless representation of A.
    * @return A decoder for A.
    */
  implicit def genericDecoder[A, ARepr <: HList](
    implicit
    gen: LabelledGeneric.Aux[A, ARepr],
    decoder: Lazy[StateDecoder[ARepr]]
  ): StateDecoder[A] = {
    createDecoder {
      decoder.value.state.map(gen.from)
    }
  }
}

/**
  * Implicits that should be used absolutely last, plus common utility methods.
  * https://stackoverflow.com/questions/1886953/is-there-a-way-to-control-which-implicit-conversion-will-be-the-default-used#answer-1887678
  */
trait LowPriorityStateDecoder {

  /**
    * Creates a constant instance of a StateDecoder[A].
    *
    * @param decoderState The state for the decoder.
    * @tparam A The type to be decoded.
    * @return The wrapped instance of a StateDecoder[A].
    */
  protected def createDecoder[A](
    decoderState: StateT[Result, ACursor, A]
  ): StateDecoder[A] = {
    new StateDecoder[A] {
      override def state: StateT[Result, ACursor, A] = decoderState
    }
  }

  /**
    * Retrieves a decoder for any HList made up of a (headNameAndType :: (more tail elements) :: HNil).
    *
    * The headNameAndType is actually made up of a FieldType, which is a tuple of [[scala.Symbol]] plus some type H.
    *
    * https://github.com/underscoreio/shapeless-guide/blob/0.3/src/pages/labelled-generic/products.md#deriving-product-instances-with-labelledgeneric
    * https://youtu.be/Zt6LjUnOcFQ?t=39m30s
    *
    * @param witness       Reads the symbol name.
    * @param tailDecoder   An implicit decoder for the tail. Either this method recursively, minus the first element, or
    *                      just the hnilDecoder.
    * @param configuration Custom extra configuration for the circe decoder, possibly snake_casing the names.
    * @tparam K The singleton-type of the head. When used with the witness allows access to the filed name.
    * @tparam H The type of the head, with a context bound also specifying that an `implicit ev: Decoder[H]` exists.
    *           https://www.scala-lang.org/files/archive/spec/2.12/07-implicits.html#context-bounds-and-view-bounds
    * @tparam T The tail of the list, some list of headNameAndType :: tail.
    * @return A decoder than can decode the head-plus-tail.
    */
  implicit def hlistDecoder[K <: Symbol, H: Decoder, T <: HList](
    implicit
    witness: Witness.Aux[K],
    tailDecoder: StateDecoder[T],
    configuration: Configuration = Configuration.default
  ): StateDecoder[FieldType[K, H] :: T] = {
    val symbol = witness.value
    val name = configuration.transformKeys(symbol.name)
    createDecoder {
      for {
        head <- Decoder.state.decodeField[H](name)
        tail <- tailDecoder.state
      } yield field[K](head) :: tail
    }
  }
}
