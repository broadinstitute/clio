package org.broadinstitute.clio.integrationtest.tests

import java.net.URI

import io.circe.literal._
import org.broadinstitute.clio.client.commands.ClioCommand
import org.broadinstitute.clio.integrationtest.BaseIntegrationSpec
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{ElasticsearchIndex}
import org.broadinstitute.clio.transfer.model.arrays.{
  ArraysExtensions,
  ArraysKey,
  ArraysMetadata
}
import org.broadinstitute.clio.util.model.{Location, UpsertId}

import scala.concurrent.Future

trait ArraysTests { self: BaseIntegrationSpec =>
  import org.broadinstitute.clio.JsonUtils.JsonOps

  def runUpsertArrays(
    key: ArraysKey,
    metadata: ArraysMetadata,
    force: Boolean = true
  ): Future[UpsertId] = {
    val tmpMetadata = writeLocalTmpJson(metadata)
    runDecode[UpsertId](
      ClioCommand.addArraysName,
      Seq(
        "--location",
        key.location.entryName,
        "--chipwell-barcode",
        key.chipwellBarcode.name,
        "--version",
        key.version.toString,
        "--metadata-location",
        tmpMetadata.toString,
        if (force) "--force" else ""
      ).filter(_.nonEmpty): _*
    )
  }

  it should "write dict, fasta, fasta.fai paths" in {
    val upsertKey = ArraysKey(
      location = Location.GCP,
      chipwellBarcode = Symbol(s"barcode$randomId"),
      version = 1
    )
    val referencesDirs = "hg19/v0/Homo_sapiens_assembly19"
    val refDictPath = rootPathForReferencesBucket / s"$referencesDirs${ArraysExtensions.DictExtension}"
    val refFastaPath = rootPathForReferencesBucket / s"$referencesDirs${ArraysExtensions.FastaExtension}"
    val refFastaIndexPath = rootPathForReferencesBucket / s"$referencesDirs${ArraysExtensions.FastaFaiExtension}"
    info(s"rootPathForReferencesBucket == ${rootPathForReferencesBucket}")
    val arraysMetadata = ArraysMetadata(
      refDictPath = Some(refDictPath.uri),
      refFastaPath = Some(refFastaPath.uri),
      refFastaIndexPath = Some(refFastaIndexPath.uri)
    )
    info(s"arraysMetadata == ${arraysMetadata}")
    for {
      upsertId <- runUpsertArrays(
        upsertKey,
        arraysMetadata,
        force = false
      )
    } yield {
      val storedDocument = getJsonFrom(upsertId)(ElasticsearchIndex.Arrays)
      info(s"storedDocument==${storedDocument}")
      storedDocument.unsafeGet[URI]("ref_dict_path") should be(refDictPath.uri)
      storedDocument.unsafeGet[URI]("ref_fasta_path") should be(refFastaPath.uri)
      storedDocument.unsafeGet[URI]("ref_fasta_index_path") should be(
        refFastaIndexPath.uri
      )
    }
  }
}
