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

package io.qalipsis.plugins.timescaledb.meter

import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.timescaledb.liquibase.LiquibaseConfiguration
import io.qalipsis.plugins.timescaledb.liquibase.LiquibaseRunner
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
        if (configuration.initSchema) {
            LiquibaseRunner(
                LiquibaseConfiguration(
                    changeLog = "db/liquibase-meters-changelog.xml",
                    host = configuration.host,
                    port = configuration.port,
                    username = configuration.username,
                    password = configuration.password,
                    database = configuration.database,
                    defaultSchemaName = configuration.schema,
                    enableSsl = configuration.enableSsl,
                    sslMode = configuration.sslMode,
                    sslRootCert = configuration.sslRootCert,
                    sslKey = configuration.sslKey,
                    sslCert = configuration.sslCert
                )
            ).run()
        }
        return connectionPool
    }

    /**
     * The bean has to be created in the context initialization phase, otherwise
     * [DbUtils.isDbTimescale] will block a non-blocking thread and generate a failure.
     */
    @Context
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