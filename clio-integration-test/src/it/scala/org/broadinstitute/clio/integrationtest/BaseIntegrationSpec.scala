package org.broadinstitute.clio.integrationtest

import java.io.ByteArrayInputStream
import java.nio.file.FileSystem
import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import better.files.File
import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.storage.StorageOptions
import com.google.cloud.storage.contrib.nio.{
  CloudStorageConfiguration,
  CloudStorageFileSystem
}
import com.sksamuel.elastic4s.http.ElasticClient
import com.sksamuel.elastic4s.http.index.mappings.IndexMappings
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json, Printer}
import org.apache.http.HttpHost
import org.broadinstitute.clio.client.ClioClient
import org.broadinstitute.clio.client.util.IoUtil
import org.broadinstitute.clio.client.webclient.ClioWebClient
import org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex
import org.broadinstitute.clio.util.auth.ClioCredentials
import org.broadinstitute.clio.util.json.ModelAutoDerivation
import org.broadinstitute.clio.util.model.UpsertId
import org.elasticsearch.client.RestClient
import org.scalatest.enablers.Existence
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
  * Base class for Clio integration tests, agnostic to the location
  * of the Clio / Elasticsearch instances that will be tested against.
  */
abstract class BaseIntegrationSpec(clioDescription: String)
    extends TestKit(ActorSystem(clioDescription.replaceAll("\\s+", "-")))
    with AsyncFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ErrorAccumulatingCirceSupport
    with LazyLogging
    with ModelAutoDerivation {

  behavior of clioDescription

  lazy implicit val m: Materializer = ActorMaterializer()

  /** URL of vault server to use when getting bearer tokens for service accounts. */
  private val vaultUrl = "https://clotho.broadinstitute.org:8200"

  /** Path in vault to the service account JSON to use in testing. */
  private val vaultPath = "secret/dsde/gotc/test/clio/clio-account.json"

  /** List of possible token-file locations, in order of preference. */
  private val vaultTokenPaths = Seq(
    File("/etc/vault-token-dsde"),
    File(System.getProperty("user.home"), ".vault-token")
  )

  /** Pull service account credentials to use when accessing cloud resources from Vault. */
  protected def loadCredentials(): GoogleCredentials = {
    val vaultToken: String = vaultTokenPaths
      .find(_.exists)
      .map(_.contentAsString.stripLineEnd)
      .getOrElse(sys.error("Vault token not found on filesystem!"))

    val vaultConfig = new VaultConfig()
      .address(vaultUrl)
      .engineVersion(1)
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

    val jsonStream = new ByteArrayInputStream(
      accountJSON.printWith(defaultPrinter).getBytes()
    )
    ServiceAccountCredentials.fromStream(jsonStream)
  }

  private lazy val clioCredentials = new ClioCredentials(loadCredentials())

  def clioHostname: String

  def clioPort: Int

  def useHttps: Boolean

  /**
    * The web client to use within the tested clio-client.
    */
  lazy val clioWebClient: ClioWebClient =
    ClioWebClient(clioCredentials, clioHostname, clioPort, useHttps)

  /**
    * The clio-client to test against.
    */
  lazy val clioClient: ClioClient =
    new ClioClient(clioWebClient, IoUtil(clioCredentials))

  /**
    * The URI of the Elasticsearch instance to test in a suite.
    * Could point to a local Docker container, or to a deployed ES node.
    */
  def elasticsearchUri: Uri

  /**
    * Get a path to the root of a bucket in cloud-storage,
    * using Google's NIO filesystem adapter.
    */
  def rootPathForBucketInEnv(env: String, readOnly: Boolean): File = {
    val project = s"broad-gotc-$env-storage"

    val gcs: FileSystem = CloudStorageFileSystem.forBucket(
      s"broad-gotc-$env-clio",
      CloudStorageConfiguration.DEFAULT,
      StorageOptions
        .newBuilder()
        .setProjectId(project)
        .setCredentials(clioCredentials.storage(readOnly))
        .build()
    )

    File(gcs.getPath("/"))
  }

  /**
    * The path to the root of the references bucket.
    */
  def rootPathForReferencesBucket: File = {
    val gcs: FileSystem = CloudStorageFileSystem.forBucket(
      s"broad-references",
      CloudStorageConfiguration.DEFAULT,
      StorageOptions
        .newBuilder()
        .build()
    )

    File(gcs.getPath("/"))
  }

  /**
    * Path to the cloud directory to which all GCP test data
    * should be written.
    */
  lazy val rootTestStorageDir: File =
    rootPathForBucketInEnv("test", readOnly = false)

  /**
    * Path to the root directory in which metadata updates will
    * be persisted.
    */
  def rootPersistenceDir: File

  /**
    * HTTP client pointing to the elasticsearch node to test against.
    *
    * Needs to be lazy because otherwise it'll raise an
    * UninitializedFieldError in the containerized spec because of
    * the initialization-order weirdness around the container field.
    */
  lazy val elasticsearchClient: ElasticClient = {
    val host = HttpHost.create(elasticsearchUri.toString())
    val restClient = RestClient.builder(host).build()
    ElasticClient.fromRestClient(restClient)
  }

  /**
    * Run a command with arbitrary args through the main clio-client.
    * Returns a failed future if the command exits early.
    */
  def runClient[Out](sink: Sink[Json, Future[Out]])(
    command: String,
    args: String*
  ): Future[Out] = {
    clioClient
      .instanceMain((command +: args).toArray, _ => ())
      .fold(
        earlyReturn =>
          Future
            .failed(new Exception(s"Command exited early with $earlyReturn")),
        _.runWith(sink)
      )
  }

  def runCollectJson(command: String, args: String*): Future[Seq[Json]] =
    runClient(Sink.seq)(command, args: _*)

  def runDecode[A: Decoder](
    command: String,
    args: String*
  ): Future[A] = {
    val decode = Flow.fromFunction[Json, A](_.as[A].fold(throw _, identity))
    runClient(decode.toMat(Sink.head)(Keep.right))(command, args: _*)
  }

  def runIgnore(command: String, args: String*): Future[Done] =
    runClient(Sink.ignore)(command, args: _*)

  /**
    * Convert one of our [[org.broadinstitute.clio.server.dataaccess.elasticsearch.ElasticsearchIndex]]
    * instances into an [[com.sksamuel.elastic4s.http.index.mappings.IndexMappings]], for comparison.
    */
  def indexToMapping(index: ElasticsearchIndex[_]): IndexMappings = {
    val fields = index.fields.map { field =>
      field.name -> {
        val fType = field.`type`
        if (fType == "text") {
          // Text fields also have a keyword sub-field.
          Map(
            "type" -> fType,
            "fields" -> Map("exact" -> Map("type" -> "keyword"))
          )
        } else {
          Map("type" -> fType)
        }
      }
    }.toMap[String, Any]

    IndexMappings(index.indexName, Map(index.indexType -> fields))
  }

  /** Get a random identifier to use as a dummy value in testing. */
  def randomId: String = UUID.randomUUID().toString.replaceAll("-", "")

  /**
    * Typeclass which enables using syntax like
    * <pre>
    * file should exist
    * </pre>
    * instead of
    * <pre>
    * file.exists should be(true)
    * </pre>
    */
  implicit val fileExistence: Existence[File] = _.exists

  /**
    * Check that the storage path for a given Clio ID exists,
    * load its contents as JSON, and check that the loaded ID
    * matches the given ID.
    */
  def getJsonFrom(expectedId: UpsertId)(
    implicit index: ElasticsearchIndex[_]
  ): Json = {
    val expectedPath = rootPersistenceDir / index.currentPersistenceDir / expectedId.persistenceFilename

    expectedPath should exist
    val json = parse(expectedPath.contentAsString).toTry.get

    ElasticsearchIndex.getUpsertId(json) should be(expectedId)
    json
  }

  /**
    * Write an object as JSON to a local temp file.
    *
    * Registers the temp file for deletion.
    */
  def writeLocalTmpJson[A: Encoder](obj: A): File = {
    writeLocalTmpFile(obj.asJson.printWith(implicitly[Printer]))
  }

  /**
    * Write a string to a local temp file.
    */
  def writeLocalTmpFile(str: String): File = {
    val tmpFile = File.newTemporaryFile("clio-integration", ".json")
    tmpFile
      .deleteOnExit()
      .write(str)
  }

  /**
    * Produce the expected value of merging the given key & metadata in Clio,
    * stripping out null values to mimic the behavior of the default printer.
    */
  def expectedMerge[K: Encoder, M: Encoder](key: K, metadata: M): Json =
    key.asJson.deepMerge(metadata.asJson.mapObject(_.filter(!_._2.isNull)))

  /** Shut down the actor system at the end of the suite. */
  override protected def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
