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

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.validation.Validated
import io.qalipsis.api.report.EventMetadataProvider
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
), EventMetadataProvider