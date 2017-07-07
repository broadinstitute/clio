package org.broadinstitute.clio.util.generic

/**
  * Converts instances from one type to another.
  *
  * @tparam From The source type.
  * @tparam To   The destination type.
  */
trait TypeConverter[From, To] {

  /**
    * Converts the value `from` from `From` to `To`.
    *
    * @param from The source instance.
    * @return The destination instance.
    */
  def convert(from: From): To
}
