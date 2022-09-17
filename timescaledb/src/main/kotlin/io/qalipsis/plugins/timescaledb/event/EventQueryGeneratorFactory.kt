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