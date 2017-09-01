package org.broadinstitute.clio.server.service

import org.broadinstitute.clio.util.generic.TypeConverter

import scala.concurrent.{ExecutionContext, Future}

import java.util.UUID

object SearchService {

  /**
    * Update-or-insert (upsert) metadata.
    *
    * @param transferKey       The DTO for the key.
    * @param transferMetadata  The DTO for the metadata.
    * @param keyConverter      Converts the key from a DTO to the internal model.
    * @param metadataConverter Converts the metadata from a DTO to the internal model.
    * @param updateMetadata    Updates the metadata.
    * @tparam TK The type of the Transfer Key DTO.
    * @tparam TM The type of the Transfer Metadata DTO.
    * @tparam MK The type of the Model Key.
    * @tparam MM The type of the Model Metadata.
    * @return A future result of the upsert.
    */
  def upsertMetadata[TK, TM, MK, MM](
    transferKey: TK,
    transferMetadata: TM,
    keyConverter: TypeConverter[TK, MK],
    metadataConverter: TypeConverter[TM, MM],
    updateMetadata: (MK, MM) => Future[UUID]
  ): Future[UUID] = {
    val modelKey = keyConverter.convert(transferKey)
    val modelMetadata = metadataConverter.convert(transferMetadata)
    updateMetadata(modelKey, modelMetadata)
  }

  /**
    * Run a query.
    *
    * @param transferInput        The DTO for the query input.
    * @param queryInputConverter  Converts the input from a DTO to the internal model.
    * @param queryOutputConverter Converts the output from the internal model to a sequence of DTOs.
    * @param query                Runs the query on the model inputs.
    * @param executionContext     Used during the mapping of outputs.
    * @tparam TI The type of the Transfer Query Input DTO.
    * @tparam TO The type of the Transfer Query Output DTO.
    * @tparam MI The type of the Model Query Input.
    * @tparam MO The type of the Model Query Output.
    * @return The result of the query.
    */
  def queryMetadata[TI, TO, MI, MO](transferInput: TI,
                                    queryInputConverter: TypeConverter[TI, MI],
                                    queryOutputConverter: TypeConverter[MO, TO],
                                    query: MI => Future[Seq[MO]])(
    implicit
    executionContext: ExecutionContext
  ): Future[Seq[TO]] = {
    val modelInput = queryInputConverter.convert(transferInput)
    val modelOutputs = query(modelInput)
    modelOutputs map {
      _ map queryOutputConverter.convert
    }
  }
}
