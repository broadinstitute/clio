package org.broadinstitute.clio.integrationtest

import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.{ClioClient, ClioClientConfig}
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.server.dataaccess.elasticsearch.{
  ClioDocument,
  ElasticsearchIndex
}
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.ServiceAccount

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.cloud.storage.StorageOptions
import com.google.cloud.storage.contrib.nio.{
  CloudStorageConfiguration,
  CloudStorageFileSystem
}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.index.mappings.IndexMappings
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.{Decoder, Encoder, Printer}
import io.circe.parser._
import io.circe.syntax._
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import java.nio.file.{FileSystem, Files, Path, Paths}
import java.util.UUID

/**
  * Base class for Clio integration tests, agnostic to the location
  * of the Clio / Elasticsearch instances that will be tested against.
  */
abstract class BaseIntegrationSpec(clioDescription: String)
    extends TestKit(ActorSystem(clioDescription.replaceAll("\\s+", "-")))
    with AsyncFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ModelAutoDerivation
    with ErrorAccumulatingCirceSupport
    with LazyLogging {

  behavior of clioDescription

  implicit val m: ActorMaterializer = ActorMaterializer()

  /**
    * Printer to override Circe's default marshalling behavior
    * for None from 'None -> null' to omitting Nones entirely.
    */
  implicit val p: Printer = Printer.noSpaces.copy(dropNullKeys = true)

  /**
    * Timeout to use for all client requests.
    *
    * Use the client's default to make sure it's sane.
    */
  val clientTimeout: FiniteDuration = ClioClientConfig.responseTimeout

  /** The bearer token to use when hitting the /api route of Clio. */
  implicit def bearerToken: OAuth2BearerToken

  /**
    * The web client to use within the tested clio-client.
    */
  def clioWebClient: ClioWebClient

  /**
    * The clio-client to test against.
    */
  lazy val clioClient: ClioClient = new ClioClient(clioWebClient, IoUtil)

  /**
    * The URI of the Elasticsearch instance to test in a suite.
    * Could point to a local Docker container, or to a deployed ES node.
    */
  def elasticsearchUri: Uri

  /** URL of vault server to use when getting bearer tokens for service accounts. */
  private val vaultUrl = "https://clotho.broadinstitute.org:8200/"

  /** Path in vault to the service account JSON to use in testing. */
  private val vaultPath = "secret/dsde/gotc/test/clio/clio-account.json"

  /** List of possible token-file locations, in order of preference. */
  private val vaultTokenPaths = Seq(
    Paths.get("/etc/vault-token-dsde"),
    Paths.get(System.getProperty("user.home"), ".vault-token")
  )

  /** Scopes needed from Google to write to our test-storage bucket. */
  private val testStorageScopes = Seq(
    "https://www.googleapis.com/auth/devstorage.read_write"
  )

  /**
    * Service account credentials for use when accessing cloud resources.
    */
  protected lazy val serviceAccount: ServiceAccount = {
    val vaultToken: String = vaultTokenPaths
      .find(Files.exists(_))
      .map { path =>
        new String(Files.readAllBytes(path)).stripLineEnd
      }
      .getOrElse {
        sys.error("Vault token not found on filesystem!")
      }

    val vaultConfig = new VaultConfig()
      .address(vaultUrl)
      .token(vaultToken)
      .build()

    val vaultDriver = new Vault(vaultConfig)
    val accountJSON =
      vaultDriver
        .logical()
        .read(vaultPath)
        .getData
        .asScala
        .toMap[String, String]
        .asJson

    accountJSON
      .as[ServiceAccount]
      .fold({ err =>
        throw new RuntimeException(
          s"Failed to decode service account JSON from Vault at $vaultPath",
          err
        )
      }, identity)
  }

  /**
    * Get a path to the root of a bucket in cloud-storage,
    * using Google's NIO filesystem adapter.
    */
  def rootPathForBucketInEnv(env: String, scopes: Seq[String]): Path = {
    val storageOptions = {
      val project = s"broad-gotc-$env-storage"
      serviceAccount
        .credentialForScopes(scopes)
        .fold(
          { err =>
            val scopesString = scopes.mkString("[", ", ", "]")
            fail(
              s"Failed to get credential for project '$project', scopes $scopesString",
              err
            )
          }, { credential =>
            StorageOptions
              .newBuilder()
              .setProjectId(project)
              .setCredentials(credential)
              .build()
          }
        )
    }

    val gcs: FileSystem = CloudStorageFileSystem.forBucket(
      s"broad-gotc-$env-clio",
      CloudStorageConfiguration.DEFAULT,
      storageOptions
    )

    gcs.getPath("/")
  }

  /**
    * Path to the cloud directory to which all GCP test data
    * should be written.
    */
  lazy val rootTestStorageDir: Path =
    rootPathForBucketInEnv("test", testStorageScopes)

  /**
    * Path to the root directory in which metadata updates will
    * be persisted.
    */
  def rootPersistenceDir: Path

  /**
    * HTTP client pointing to the elasticsearch node to test against.
    *
    * Needs to be lazy because otherwise it'll raise an
    * UninitializedFieldError in the containerized spec because of
    * the initialization-order weirdness around the container field.
    */
  lazy val elasticsearchClient: HttpClient = {
    val host = HttpHost.create(elasticsearchUri.toString())
    val restClient = RestClient.builder(host).build()
    HttpClient.fromRestClient(restClient)
  }

  /**
    * Run a command with arbitrary args through the main clio-client.
    * Returns a failed future if the command exits early.
    */
  def runClient(command: String, args: String*): Future[HttpResponse] = {
    clioClient
      .instanceMain(
        (Seq("--bearer-token", bearerToken.token, command) ++ args).toArray
      )
      .fold(
        earlyReturn =>
          Future
            .failed(new Exception(s"Command exited early with $earlyReturn")),
        identity
      )
  }

  /**
    * Convert one of our [[org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex]]
    * instances into an [[com.sksamuel.elastic4s.http.index.mappings.IndexMappings]], for comparison.
    */
  def indexToMapping(index: ElasticsearchIndex[_]): IndexMappings = {
    val fields = index.fields
      .map { field =>
        field.name -> Map("type" -> field.`type`)
      }
      .toMap[String, Any]

    IndexMappings(index.indexName, Map(index.indexType -> fields))
  }

  /** Get a random identifier to use as a dummy value in testing. */
  def randomId: String = UUID.randomUUID().toString.replaceAll("-", "")

  /**
    * Check that the storage path for a given Clio ID exists,
    * load its contents as JSON, and check that the loaded ID
    * matches the given ID.
    */
  def getJsonFrom[Document <: ClioDocument: Decoder](
    index: ElasticsearchIndex[Document],
    upsertId: UUID
  ): Document = {
    val expectedPath =
      rootPersistenceDir.resolve(
        s"${index.currentPersistenceDir}/$upsertId.json"
      )

    Files.exists(expectedPath) should be(true)
    val document = parse(new String(Files.readAllBytes(expectedPath)))
      .flatMap(_.as[Document])
      .toTry
      .get

    document.upsertId should be(upsertId)
    document
  }

  /**
    * Write an object as JSON to a local temp file.
    *
    * Registers the temp file for deletion.
    */
  def writeLocalTmpJson[A: Encoder](obj: A): Path = {
    val tmpFile = Files.createTempFile("clio-integration", ".json")
    val json = obj.asJson.pretty(implicitly[Printer])
    Files.write(tmpFile, json.getBytes)

    tmpFile.toFile.deleteOnExit()
    tmpFile
  }

  /** Shut down the actor system at the end of the suite. */
  override protected def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
