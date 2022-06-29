package io.qalipsis.plugins.timescaledb.utils

import io.qalipsis.plugins.timescaledb.dataprovider.DataProviderConfiguration
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.Connection
import reactor.core.publisher.Flux

internal object DbUtils {

    /**
     * Verifies whether the underlying database server is TimescaleDB.
     *
     * The verification is made by checking the existence of the TimescaleDB function create_hypertable.
     */
    fun isDbTimescale(connectionPool: ConnectionPool): Boolean {
        return Flux.usingWhen(
            connectionPool.create(),
            { connection ->
                Flux.from(
                    connection
                        .createStatement("select count(*) > 0 as \"isTimescale\" from pg_proc where proname = 'create_hypertable'")
                        .execute()
                ).flatMap { result ->
                    result.map { row, _ -> row.get("isTimescale") as Boolean? }
                }
            },
            Connection::close
        ).blockFirst() ?: false
    }

    /**
     * Creates a new pool of R2DBC connections for the provided configuration.
     */
    fun createConnectionPool(configuration: DataProviderConfiguration): ConnectionPool {
        val connectionFactory = PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host(configuration.host)
                .port(configuration.port)
                .username(configuration.username)
                .password(configuration.password)
                .database(configuration.database)
                .schema(configuration.schema)
                .applicationName("qalipsis-timescaledb")
                .build()
        )

        val poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
            .initialSize(configuration.minSize)
            .maxSize(configuration.maxSize)
            .maxIdleTime(configuration.maxIdleTime)
            .build()

        return ConnectionPool(poolConfiguration)
    }

}