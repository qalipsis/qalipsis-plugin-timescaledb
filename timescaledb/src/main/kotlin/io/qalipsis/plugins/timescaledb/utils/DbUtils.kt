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