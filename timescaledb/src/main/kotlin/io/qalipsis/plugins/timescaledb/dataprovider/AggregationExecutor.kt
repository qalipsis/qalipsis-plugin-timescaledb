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

package io.qalipsis.plugins.timescaledb.dataprovider

import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.query.AggregationQueryExecutionContext
import io.qalipsis.api.report.TimeSeriesAggregationResult
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.spi.Connection
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime

/**
 * Specific class to execute a single aggregation on time-series data.
 *
 * @property connectionPool the pool of connection to the DB to use
 * @property context the context of the current execution
 * @property statement the SQL statement to complete for execution
 * @property boundParameters the parameters to bind to the string placeholders and prepared statement
 * @property nextParameterIndex next available index to use to add dynamic clauses to the request
 *
 * @author Eric Jessé
 */
internal class AggregationExecutor(
    private val connectionPool: ConnectionPool,
    private val context: AggregationQueryExecutionContext,
    private val statement: String,
    private val boundParameters: Map<String, BoundParameter>,
    private val nextParameterIndex: Int
) : AbstractQueryExecutor<List<TimeSeriesAggregationResult>>() {

    override suspend fun execute(): List<TimeSeriesAggregationResult> {
        val actualTimeframe = context.aggregationTimeframe.toMillis()
        val (actualStart, actualEnd) = roundStartAndEnd(actualTimeframe, context.from, context.until)

        val actualBoundParameters = boundParameters.toMutableMap()
        val additionalClauses = buildAdditionalClauses(
            campaignsReferences = context.campaignsReferences,
            scenariosNames = context.scenariosNames,
            actualBoundParameters = actualBoundParameters,
            nextParameterIndex = nextParameterIndex
        )

        val sqlStatement =
            String.format(statement.replace("%timeframe%", "$actualTimeframe"), additionalClauses.toString())
        log.trace { "Executing the prepared statement\n\t$sqlStatement \n\twith the bound parameters\n\t$actualBoundParameters" }
        return Flux.usingWhen(
            connectionPool.create(),
            { connection ->
                Mono.from(connection.createStatement(sqlStatement).also { statement ->
                    bindArguments(context.tenant, statement, actualBoundParameters, actualStart, actualEnd)
                }.execute())
                    .flatMapMany { result ->
                        log.trace { "Received a result to the query" }
                        var firstBucket: Instant = Instant.EPOCH
                        result.map { row, _ ->
                            val timestamp = (row["bucket"] as OffsetDateTime).toInstant()
                            val value = (row["result"] as Number?)?.let {
                                when (it) {
                                    is BigDecimal -> it
                                    is Long -> BigDecimal.valueOf(
                                        it
                                    )
                                    is Double -> BigDecimal.valueOf(it)
                                    else -> null
                                }
                            }

                            val elapsed = if (firstBucket == Instant.EPOCH) {
                                firstBucket = timestamp
                                Duration.ZERO
                            } else {
                                Duration.between(firstBucket, timestamp)
                            }
                            TimeSeriesAggregationResult(
                                start = timestamp,
                                elapsed = elapsed,
                                campaign = row["campaign"] as? String,
                                value = value
                            ).also {
                                log.trace { "Received the aggregation $it" }
                            }
                        }
                    }
            },
            Connection::close
        ).asFlow().toList(mutableListOf<TimeSeriesAggregationResult>())
    }

    private companion object {
        val log = logger()
    }
}