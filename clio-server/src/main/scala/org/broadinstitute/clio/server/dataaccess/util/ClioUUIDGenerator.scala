package org.broadinstitute.clio.server.dataaccess.util

import com.fasterxml.uuid.impl.TimeBasedGenerator
import com.fasterxml.uuid.{EthernetAddress, Generators}

import java.util.UUID

/**
  * We are using a UUID generator to generate time-based UUIDs for every Clio upsert.
  * This is used for debugging and is not analyzed in ElasticSearch
  */
object ClioUUIDGenerator {

  private val generator: TimeBasedGenerator =
    Generators.timeBasedGenerator(EthernetAddress.fromInterface())

  /*
   * This is a thunk function value, instead of a nullary method,
   * so we can pass it around without dealing with the warning / error
   * nonsense surrounding eta expansion of zero-arg methods.
   */
  val getUUID: () => UUID = () => generator.generate()

}
