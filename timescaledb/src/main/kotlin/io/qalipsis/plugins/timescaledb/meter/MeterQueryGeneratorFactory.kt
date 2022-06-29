package io.qalipsis.plugins.timescaledb.meter

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
    Requires(beans = [TimescaledbMeterDataProviderConfiguration::class])
)
internal class MeterQueryGeneratorFactory {

    private lateinit var connectionPool: ConnectionPool

    @Singleton
    @Named("meter-data-provider")
    fun meterDataProviderConnection(configuration: TimescaledbMeterDataProviderConfiguration): ConnectionPool {
        connectionPool = DbUtils.createConnectionPool(configuration)
        return connectionPool
    }

    @Singleton
    fun meterQueryGenerator(@Named("meter-data-provider") connectionPool: ConnectionPool): AbstractMeterQueryGenerator {
        return if (DbUtils.isDbTimescale(connectionPool)) {
            log.info { "Using TimescaleDB as meter data provider" }
            TimescaledbMeterQueryGenerator()
        } else {
            log.info { "Using PostgreSQL as meter data provider" }
            PostgresMeterQueryGenerator()
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