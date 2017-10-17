package org.broadinstitute.clio.util.generic

import s_mach.string.{LexResult, Lexer, RegexCharTransitionLexer}

/**
  * An s_mach lexer for converting camel-case to snake-case using the same
  * regex logic as circe.
  *
  * By default, s_mach only breaks camel-case through a single type of "boundary" detection:
  *
  *   ([a-z])([A-Z]) -> \$1_\$2
  *
  * Circe, on the other hand, breaks camel case at two types of boundaries:
  *
  *   ([A-Z]+)([A-Z][a-z]) -> \$1_\$2
  *   ([a-z\\d])([A-Z]) -> \$1_\$2
  *
  * In practice, this difference caused discrepancies between how circe serialized certain
  * field names and how s_mach built the fields' corresponding elasticsearch mappings:
  *
  *   selfSMPath -> self_sm_path (circe) VS. self_smpath (s_mach)
  *   cramMd5Path -> cram_md5_path (circe) VS. cram_md5path (s_mach)
  *
  * The circe paths seem more sane, so we customize s_mach to match.
  */
object CirceEquivalentCamelCaseLexer extends Lexer {
  private val delegate = RegexCharTransitionLexer(
    Seq(("[A-Z]+".r, "[A-Z][a-z]".r), ("[a-z\\d]".r, "[A-Z]".r))
  )
  override def tokens(s: String): Iterator[String] = delegate.tokens(s)
  override def lex(s: String): LexResult = delegate.lex(s)
}
