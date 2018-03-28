package com.google.auth.oauth2

import com.google.api.client.testing.http.FixedClock
import com.google.auth.oauth2.OAuth2Credentials.CredentialsChangedListener
import com.typesafe.scalalogging.LazyLogging

class TimeChangingChangeListener(timeToSet: Long)
    extends CredentialsChangedListener
    with LazyLogging {
  override def onChanged(credentials: OAuth2Credentials): Unit = {
    logger.info(s"Changing time for credentials to $timeToSet")
    credentials.clock = new FixedClock(timeToSet)
  }
}
