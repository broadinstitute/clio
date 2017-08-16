package org.broadinstitute.clio.client.parser

import java.time.OffsetDateTime

import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.Commands

import scala.concurrent.ExecutionContext

case class BaseArgs(command: Option[Commands.Command] = None,
                    bearerToken: String = "",
                    metadataLocation: String = "",
                    flowcell: Option[String] = None,
                    lane: Option[Int] = None,
                    libraryName: Option[String] = None,
                    location: Option[String] = None,
                    lcSet: Option[String] = None,
                    project: Option[String] = None,
                    sampleAlias: Option[String] = None,
                    runDateEnd: Option[OffsetDateTime] = None,
                    runDateStart: Option[OffsetDateTime] = None)

class BaseParser(implicit ec: ExecutionContext)
    extends scopt.OptionParser[BaseArgs]("clio-client") {
  override def showUsageOnError = true

  head("\nclio-client", ClioClientConfig.Version.value + "\n")

  opt[String]('t', "bearer-token")
    .optional()
    .action((token, conf) => conf.copy(bearerToken = token))
    .text("A valid bearer token for authentication.")

  cmd(Commands.AddReadGroupBam.apply().commandName)
    .action((_, c) => c.copy(command = Some(Commands.AddReadGroupBam.apply())))
    .text("This command is used to add a read group group bam.")
    .children(
      opt[String]('m', "meta-data-file")
        .required()
        .action(
          (metadataLocation, config) =>
            config.copy(metadataLocation = metadataLocation)
        )
        .text(
          "The location of the file or object containing bam metadata. (Required)"
        ),
      opt[String]('f', "flowcell")
        .required()
        .action((flowcell, config) => config.copy(flowcell = Some(flowcell)))
        .text("The flowcell for this read group. (Required)"),
      opt[Int]('l', "lane")
        .required()
        .action((lane, config) => config.copy(lane = Some(lane)))
        .text("The lane for this read group. (Required)"),
      opt[String]('n', "libraryName")
        .required()
        .action(
          (libraryName, config) => config.copy(libraryName = Some(libraryName))
        )
        .text("The library name for this read group. (Required)"),
      opt[String]("location")
        .required()
        .action((location, config) => config.copy(location = Some(location)))
        .text("The location for this read group. (Required)")
    )

  cmd(Commands.QueryReadGroupBam.apply().commandName)
    .action(
      (_, c) => c.copy(command = Some(Commands.QueryReadGroupBam.apply()))
    )
    .text("This command is used to query a read group group bam.")
    .children(
      opt[String]('f', "flowcell")
        .optional()
        .action((flowcell, config) => config.copy(flowcell = Some(flowcell)))
        .text("The flowcell for this read group."),
      opt[Int]('l', "lane")
        .optional()
        .action((lane, config) => config.copy(lane = Some(lane)))
        .text("The lane for this read group."),
      opt[String]('n', "libraryName")
        .optional()
        .action(
          (libraryName, config) => config.copy(libraryName = Some(libraryName))
        )
        .text("The library name for this read group."),
      opt[String]("location")
        .optional()
        .action((location, config) => config.copy(location = Some(location)))
        .text("The location for this read group."),
      opt[String]("lc-set")
        .optional()
        .action((lcSet, config) => config.copy(lcSet = Some(lcSet)))
        .text("The lcSet for this read group."),
      opt[String]("sample-alias")
        .optional()
        .action(
          (sampleAlias, config) => config.copy(sampleAlias = Some(sampleAlias))
        )
        .text("The sample alias for this read group."),
      opt[String]("project")
        .optional()
        .action((project, config) => config.copy(project = Some(project)))
        .text("The project for this read group.")
    )

  checkConfig { config =>
    if (config.command.isDefined) success
    else failure("A command is required.")
  }
}
