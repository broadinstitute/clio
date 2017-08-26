package org.broadinstitute.clio.server.service.util

import java.util.UUID

import com.fasterxml.uuid.{EthernetAddress, Generators}
import com.fasterxml.uuid.impl.TimeBasedGenerator

/**
  * We are using a UUID generator to generate time-based UUIDs for every Clio upsert.
  * This is used for debugging and is not analyzed in ElasticSearch
  */
object ClioUUIDGenerator {

  val generator: TimeBasedGenerator =
    Generators.timeBasedGenerator(EthernetAddress.fromInterface())

  def getUUID(): UUID = generator.generate()

}
