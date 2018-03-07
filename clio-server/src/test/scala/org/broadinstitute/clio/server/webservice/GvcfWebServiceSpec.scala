package org.broadinstitute.clio.server.webservice

import java.net.URI

import org.broadinstitute.clio.server.MockClioApp
import org.broadinstitute.clio.server.dataaccess.elasticsearch.DocumentGvcf
import org.broadinstitute.clio.util.model.{DocumentStatus, Location}
import org.broadinstitute.clio.transfer.model.GvcfIndex
import org.broadinstitute.clio.transfer.model.gvcf.{GvcfExtensions, TransferGvcfV1Key, TransferGvcfV1QueryInput}
import org.broadinstitute.clio.util.generic.CaseClassMapper
import org.scalatest.Assertion

class GvcfWebServiceSpec extends IndexWebServiceSpec[GvcfIndex.type, DocumentGvcf] {

  val webServiceName = "GvcfWebService"
  val format = "gvcf"
  val webService = new MockGvcfWebService(
    MockClioApp(searchDAO = memorySearchDAO)
  )
  val queryInputWithProjectAndDocumentStatus = TransferGvcfV1QueryInput(
    project = Some("proj0"),
    documentStatus = Some(DocumentStatus.Normal)
  )
  val queryInputWithLocation: TransferGvcfV1QueryInput = TransferGvcfV1QueryInput(
    location = Some(Location.GCP)
  )
  val queryInputWithLocationAndDocumentStatus = TransferGvcfV1QueryInput(
    location = Some(Location.GCP),
    documentStatus = Some(DocumentStatus.Normal)
  )
  val onPremKey = TransferGvcfV1Key(
    Location.OnPrem,
    "project",
    "sample_alias",
    1
  )
  val cloudKey: TransferGvcfV1Key = onPremKey
    .copy(
      location = Location.GCP
    )
  val metadataMap = Map(
    "gvcf_md5" -> "abcgithashdef",
    "notes" -> "some note",
    "gvcf_path" -> s"gs://path/gvcf${GvcfExtensions.GvcfExtension}"
  )

  def getInputValsMap(queryInput: TransferGvcfV1QueryInput): Map[String, _] = {
    new CaseClassMapper[TransferGvcfV1QueryInput].vals(queryInput)
  }

  def updateCallShouldMatchMetadataMap(document: DocumentGvcf, map: Map[String, String]): Assertion = {
    map.get("document_status") match {
      case Some(d) => document.documentStatus should be(d)
    }
    document.gvcfMd5 should be(map.get("gvcf_md5").map(Symbol(_)))
    document.gvcfPath should be(map.get("gvcf_path").map(URI.create))
    document.notes should be(map.get("notes"))
  }
}
