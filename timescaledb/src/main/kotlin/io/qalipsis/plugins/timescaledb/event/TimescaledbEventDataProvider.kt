package io.qalipsis.plugins.timescaledb.event

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.validation.Validated
import io.qalipsis.api.report.EventProvider
import io.qalipsis.plugins.timescaledb.dataprovider.AbstractDataProvider
import io.r2dbc.pool.ConnectionPool
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Implementation of [AbstractDataProvider] for the events in TimescaleDB.
 *
 * @author Eric Jessé
 */
@Singleton
@Requirements(
    Requires(env = ["standalone", "head"]),
    Requires(beans = [TimescaledbEventDataProviderConfiguration::class])
)
@Validated
internal class TimescaledbEventDataProvider(
    @Named("event-data-provider") connectionPool: ConnectionPool,
    eventQueryGenerator: AbstractEventQueryGenerator,
    objectMapper: ObjectMapper
) : AbstractDataProvider(
    connectionPool = connectionPool,
    databaseTable = "events",
    queryGenerator = eventQueryGenerator,
    objectMapper = objectMapper
), EventProvider