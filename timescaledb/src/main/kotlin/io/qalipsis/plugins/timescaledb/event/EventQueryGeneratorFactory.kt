package io.qalipsis.plugins.timescaledb.event

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.timescaledb.utils.DbUtils
import io.r2dbc.pool.ConnectionPool
import jakarta.inject.Named
import jakarta.inject.Singleton
import javax.annotation.PreDestroy

@Factory
@Requirements(
    Requires(env = ["standalone", "head"]),
    Requires(beans = [TimescaledbEventDataProviderConfiguration::class])
)
internal class EventQueryGeneratorFactory {

    private lateinit var connectionPool: ConnectionPool

    @Singleton
    @Named("event-data-provider")
    fun eventDataProviderConnection(configuration: TimescaledbEventDataProviderConfiguration): ConnectionPool {
        connectionPool = DbUtils.createConnectionPool(configuration)
        return connectionPool
    }

    @Singleton
    fun eventQueryGenerator(@Named("event-data-provider") connectionPool: ConnectionPool): AbstractEventQueryGenerator {
        return if (DbUtils.isDbTimescale(connectionPool)) {
            log.info { "Using TimescaleDB as event data provider" }
            TimescaledbEventQueryGenerator()
        } else {
            log.info { "Using PostgreSQL as event data provider" }
            PostgresEventQueryGenerator()
        }
    }

    @PreDestroy
    fun close() {
        connectionPool.dispose()
    }

    private companion object {
        val log = logger()
    }
}