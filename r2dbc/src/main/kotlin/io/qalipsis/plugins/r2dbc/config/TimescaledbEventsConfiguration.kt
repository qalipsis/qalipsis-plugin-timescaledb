package io.qalipsis.plugins.r2dbc.config

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.core.bind.annotation.Bindable
import io.qalipsis.api.events.EventLevel
import io.qalipsis.plugins.r2dbc.events.TimescaledbEventsPublisher
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
@Requires(property = "events.export.timescaledb.enabled", value = "true")
@ConfigurationProperties("events.export.timescaledb")
interface TimescaledbEventsConfiguration {

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

    @get:Bindable(defaultValue = "qalipsis_ts")
    @get:NotBlank
    val schema: String

    @get:Bindable(defaultValue = "10S")
    val lingerPeriod: Duration

    @get:Min(1)
    @get:Bindable(defaultValue = "40000")
    val batchSize: Int

    @get:Min(1)
    @get:Bindable(defaultValue = "1")
    val publishers: Int

    @get:NotBlank
    val username: String

    @get:NotBlank
    val password: String
}
