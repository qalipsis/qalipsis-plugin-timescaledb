package io.qalipsis.plugins.timescaledb.meter

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.validation.Validated
import io.qalipsis.api.report.MeterMetadataProvider
import io.qalipsis.plugins.timescaledb.dataprovider.AbstractDataProvider
import io.r2dbc.pool.ConnectionPool
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Implementation of [AbstractDataProvider] for the meters in TimescaleDB.
 *
 * @author Eric Jessé
 */
@Singleton
@Requirements(
    Requires(env = ["standalone", "head"]),
    Requires(beans = [TimescaledbMeterDataProviderConfiguration::class])
)
@Validated
internal class TimescaledbMeterDataProvider(
    @Named("meter-data-provider") connectionPool: ConnectionPool,
    meterQueryGenerator: AbstractMeterQueryGenerator,
    objectMapper: ObjectMapper
) : AbstractDataProvider(
    connectionPool = connectionPool,
    databaseTable = "meters",
    queryGenerator = meterQueryGenerator,
    objectMapper = objectMapper
), MeterMetadataProvider