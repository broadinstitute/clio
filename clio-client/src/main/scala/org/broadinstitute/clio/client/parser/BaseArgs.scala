package org.broadinstitute.clio.client.parser

import java.time.OffsetDateTime

import org.broadinstitute.clio.client.ClioClientConfig
import org.broadinstitute.clio.client.commands.Commands
import org.broadinstitute.clio.util.model.DocumentStatus

case class BaseArgs(command: Option[String] = None,
                    bearerToken: String = "",
                    metadataLocation: String = "",
                    flowcell: Option[String] = None,
                    lane: Option[Int] = None,
                    libraryName: Option[String] = None,
                    location: Option[String] = None,
                    lcSet: Option[String] = None,
                    project: Option[String] = None,
                    sampleAlias: Option[String] = None,
                    documentStatus: Option[DocumentStatus] = None,
                    runDateEnd: Option[OffsetDateTime] = None,
                    runDateStart: Option[OffsetDateTime] = None)

class BaseParser extends scopt.OptionParser[BaseArgs]("clio-client") {
  override def showUsageOnError = true

  head("\nclio-client", ClioClientConfig.Version.value + "\n")

  opt[String]('t', "bearer-token")
    .optional()
    .action((token, conf) => conf.copy(bearerToken = token))
    .text("A valid bearer token for authentication.")

  cmd(Commands.addWgsUbam)
    .action((_, c) => c.copy(command = Some(Commands.addWgsUbam)))
    .text("This command is used to add a wgs ubam (WGS unmapped bam).")
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
        .text("The flowcell for this wgs ubam (WGS unmapped bam). (Required)"),
      opt[Int]('l', "lane")
        .required()
        .action((lane, config) => config.copy(lane = Some(lane)))
        .text("The lane for this wgs ubam (WGS unmapped bam). (Required)"),
      opt[String]('n', "libraryName")
        .required()
        .action(
          (libraryName, config) => config.copy(libraryName = Some(libraryName))
        )
        .text(
          "The library name for this wgs ubam (WGS unmapped bam). (Required)"
        ),
      opt[String]("location")
        .required()
        .action((location, config) => config.copy(location = Some(location)))
        .text("The location for this wgs ubam (WGS unmapped bam). (Required)")
    )

  cmd(Commands.queryWgsUbam)
    .action((_, c) => c.copy(command = Some(Commands.queryWgsUbam)))
    .text("This command is used to query a wgs ubam (WGS unmapped bam).")
    .children(
      opt[String]('f', "flowcell")
        .optional()
        .action((flowcell, config) => config.copy(flowcell = Some(flowcell)))
        .text("The flowcell for this wgs ubam (WGS unmapped bam)."),
      opt[Int]('l', "lane")
        .optional()
        .action((lane, config) => config.copy(lane = Some(lane)))
        .text("The lane for this wgs ubam (WGS unmapped bam)."),
      opt[String]('n', "libraryName")
        .optional()
        .action(
          (libraryName, config) => config.copy(libraryName = Some(libraryName))
        )
        .text("The library name for this wgs ubam (WGS unmapped bam)."),
      opt[String]("location")
        .optional()
        .action((location, config) => config.copy(location = Some(location)))
        .text("The location for this wgs ubam (WGS unmapped bam)."),
      opt[String]("lc-set")
        .optional()
        .action((lcSet, config) => config.copy(lcSet = Some(lcSet)))
        .text("The lcSet for this wgs ubam (WGS unmapped bam)."),
      opt[String]("sample-alias")
        .optional()
        .action(
          (sampleAlias, config) => config.copy(sampleAlias = Some(sampleAlias))
        )
        .text("The sample alias for this wgs ubam (WGS unmapped bam)."),
      opt[String]("project")
        .optional()
        .action((project, config) => config.copy(project = Some(project)))
        .text("The project for this wgs ubam (WGS unmapped bam).")
    )

  checkConfig { config =>
    if (config.command.isDefined) success
    else failure("A command is required.")
  }
}