package org.broadinstitute.clio.client.commands

import akka.http.scaladsl.model.HttpResponse
import io.circe.parser.parse
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClientAutoDerivation._
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.transfer.model.TransferWgsUbamV1Metadata

import scala.concurrent.{ExecutionContext, Future}

class AddWgsUbamExecutor(addWgsUbam: AddWgsUbam) extends Executor {
  override def execute(webClient: ClioWebClient, bearerToken: String)(
    implicit ec: ExecutionContext
  ): Future[HttpResponse] = {
    val decodedOrError = parse(IoUtil.readMetadata(addWgsUbam.metadataLocation))
      .flatMap(_.as[TransferWgsUbamV1Metadata])

    decodedOrError.fold(Future.failed, { decoded =>
      webClient.addWgsUbam(
        bearerToken = bearerToken,
        transferWgsUbamV1Metadata = decoded,
        input = addWgsUbam.transferWgsUbamV1Key
      )
    })
  }
}
