package org.broadinstitute.clio.server.model

case class ModelMockKey(mockKey1: String, mockKey2: Long)

case class ModelMockMetadata(mockField1: Option[Double],
                             mockField2: Option[Int])

case class ModelMockData(mockFileMd5: String,
                         mockFilePath: String,
                         mockFileSize: Long)

case class ModelMockQueryInput(mockKey1: Option[String],
                               mockKey2: Option[Long],
                               mockField1: Option[Double],
                               mockField2: Option[Int])

case class ModelMockQueryOutput(mockKey1: String,
                                mockKey2: Long,
                                mockField1: Option[Double],
                                mockField2: Option[Int],
                                mockFileMd5: Option[String],
                                mockFilePath: Option[String],
                                mockFileSize: Option[Long])
