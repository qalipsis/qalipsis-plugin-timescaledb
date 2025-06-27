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

package io.qalipsis.plugins.timescaledb.utils

import io.qalipsis.plugins.timescaledb.dataprovider.DataProviderConfiguration
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.Connection
import kotlinx.coroutines.reactive.awaitFirst
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
                .also { builder ->
                    if (configuration.enableSsl) {
                        builder.enableSsl()
                            .sslMode(configuration.sslMode)
                        configuration.sslRootCert?.let { builder.sslRootCert(it) }
                        configuration.sslCert?.let { builder.sslCert(it) }
                        configuration.sslKey?.let { builder.sslKey(it) }
                    }
                }
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


    /**
     * Fetch the used space by a tenant, in bytes.
     */
    suspend fun fetchStorage(
        connectionPool: ConnectionPool, tenant: String, schema: String, databaseTable: String
    ): Long {
        val sql = StringBuilder(
            """SELECT 
                CASE 
                    WHEN total_records = 0 THEN 0
                    ELSE (total_size_bytes * tenant_records / total_records)
                END AS used 
                FROM
                    (SELECT pg_total_relation_size('$schema.$databaseTable') AS total_size_bytes) AS total_size_bytes,
                    (SELECT COUNT(*) AS total_records FROM $schema.$databaseTable) AS total_records,
                    (SELECT count(1) AS tenant_records FROM $schema.$databaseTable WHERE tenant = '$tenant') AS tenant_records
            """
        )
        return Flux.usingWhen(
            connectionPool.create(), { connection ->
                Flux.from(
                    connection.createStatement(sql.toString()).execute()
                ).flatMap { result ->
                    result.map { row, _ -> row.get("used") as Long }
                }
            },
            Connection::close
        ).awaitFirst()
    }

}