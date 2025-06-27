/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
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
