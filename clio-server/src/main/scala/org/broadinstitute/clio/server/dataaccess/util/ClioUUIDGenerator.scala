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

  def getUUID(): UUID = generator.generate()

}
