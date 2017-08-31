package org.broadinstitute.clio.server.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

trait SwaggerDirectives {

  lazy val swaggerRoutes: Route = concat(swaggerUiService, swaggerApiDocs)

  private val swaggerUiPath = "META-INF/resources/webjars/swagger-ui/3.1.5"

  private val swaggerUiBaseUrl = "swagger"

  private val swaggerUiDocsPath = "api-docs"

  private[webservice] val swaggerUiService: Route = {
    pathPrefix(swaggerUiBaseUrl) {
      pathEndOrSingleSlash {
        redirect(
          s"/$swaggerUiBaseUrl/index.html?url=/$swaggerUiDocsPath",
          StatusCodes.TemporaryRedirect
        )
      } ~
        getFromResourceDirectory(swaggerUiPath)
    }
  }

  private[webservice] val swaggerApiDocs: Route = {
    path(swaggerUiDocsPath) {
      getFromResource(s"$swaggerUiBaseUrl/api-docs.yaml")
    }
  }

}
