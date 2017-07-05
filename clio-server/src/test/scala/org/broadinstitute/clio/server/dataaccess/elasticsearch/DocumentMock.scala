package org.broadinstitute.clio.server.dataaccess.elasticsearch

case class DocumentMock(mockField1: Option[Double],
                        mockField2: Option[Int],
                        mockFileMd5: Option[String],
                        mockFilePath: Option[String],
                        mockFileSize: Option[Long],
                        mockKey1: String,
                        mockKey2: Long)
