package io.qalipsis.plugins.r2dbc.config

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.constraints.PositiveDuration
import io.qalipsis.api.events.EventLevel
import java.time.Duration
import javax.validation.constraints.Min
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull
import io.qalipsis.plugins.r2dbc.events.TimescaledbEventsPublisher

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
internal class TimescaledbEventsConfiguration {

    @field:NotNull
    var minLevel: EventLevel = EventLevel.INFO

    @field:NotEmpty
    var host: List<@NotBlank String> = listOf("http://localhost:5432")

    @field:PositiveDuration
    var lingerPeriod: Duration = Duration.ofSeconds(10)

    @field:Min(1)
    var batchSize: Int = 40000

    @field:Min(1)
    var publishers: Int = 1

    var username: String? = null

    var password: String? = null
}
