/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.timescaledb.event

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.core.bind.annotation.Bindable
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.events.EventLevel
import io.r2dbc.postgresql.client.SSLMode
import java.time.Duration
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotNull
import javax.validation.constraints.Positive

/**
 * Configuration for [TimescaledbEventsPublisher].
 *
 * @property minLevel minimal accepted level of events defaults to INFO.
 * @property host host to connect to the Postgres, defaults to http://localhost:5432.
 * @property lingerPeriod maximal period between two publication of events to Postgres defaults to 10 seconds.
 * @property batchSize maximal number of events buffered between two publications of events to Postgres defaults to 2000.
 * @property publishers number of concurrent publication of events that can be run defaults to 1 (no concurrency).
 * @property username name of the user to use for basic authentication when connecting to Postgres.
 * @property password password of the user to use for basic authentication when connecting to Postgres.
 *
 * @author Gabriel Moraes
 */
@Requires(property = "events.export.timescaledb.enabled", value = StringUtils.TRUE, defaultValue = StringUtils.FALSE)
@ConfigurationProperties("events.export.timescaledb")
interface TimescaledbEventsPublisherConfiguration {

    @get:Bindable(defaultValue = "INFO")
    @get:NotNull
    val minLevel: EventLevel

    @get:Bindable(defaultValue = "localhost")
    @get:NotBlank
    val host: String

    @get:Bindable(defaultValue = "5432")
    @get:Positive
    val port: Int

    @get:Bindable(defaultValue = "qalipsis")
    @get:NotBlank
    val database: String

    @get:Bindable(defaultValue = "meters")
    @get:NotBlank
    val schema: String

    @get:Bindable(defaultValue = "10S")
    val lingerPeriod: Duration

    @get:Min(1)
    @get:Bindable(defaultValue = "20000")
    val batchSize: Int

    @get:Min(1)
    @get:Bindable(defaultValue = "1")
    val publishers: Int

    @get:NotBlank
    val username: String

    @get:NotBlank
    val password: String

    @get:Bindable(defaultValue = "false")
    val enableSsl: Boolean

    @get:Bindable(defaultValue = "PREFER")
    val sslMode: SSLMode

    val sslRootCert: String?

    val sslCert: String?

    val sslKey: String?

    /**
     * Specifies whether the schema for the meters should be created or updated at startup.
     */
    @get:Bindable(defaultValue = "true")
    val initSchema: Boolean
}
