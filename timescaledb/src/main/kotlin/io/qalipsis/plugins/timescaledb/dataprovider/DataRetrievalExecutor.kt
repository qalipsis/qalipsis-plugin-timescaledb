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

package io.qalipsis.plugins.timescaledb.dataprovider

import io.qalipsis.api.query.DataRetrievalQueryExecutionContext
import io.qalipsis.api.query.Page
import io.qalipsis.api.report.TimeSeriesRecord
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.spi.Connection
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Instant
import kotlin.math.ceil

/**
 * Specific class to retrieve records of time-series data.
 *
 * @property connectionPool the pool of connection to the DB to use
 * @property context the context of the current execution
 * @property countStatement the SQL statement to complete to count the total number of records
 * @property countStatement the SQL statement to complete to retrieve the expected page of records
 * @property boundParameters the parameters to bind to the string placeholders and prepared statement
 * @property nextParameterIndex next available index to use to add dynamic clauses to the request
 *
 * @author Eric Jessé
 */
internal class DataRetrievalExecutor(
    private val ioCoroutineScope: CoroutineScope,
    private val converter: TimeSeriesRecordConverter,
    private val connectionPool: ConnectionPool,
    private val context: DataRetrievalQueryExecutionContext,
    private val countStatement: String,
    private val selectStatement: String,
    private val boundParameters: Map<String, BoundParameter>,
    private val nextParameterIndex: Int,
    private val dataType: DataType? = null
) : AbstractQueryExecutor<Page<TimeSeriesRecord>>() {

    override suspend fun execute(): Page<TimeSeriesRecord> {
        val actualTimeframe = context.aggregationTimeframe.toMillis()
        val (actualStart, actualEnd) = roundStartAndEnd(actualTimeframe, context.from, context.until)

        val actualBoundParameters = boundParameters.toMutableMap()
        val additionalClauses = buildAdditionalClauses(
            campaignsReferences = context.campaignsReferences,
            scenariosNames = context.scenariosNames,
            actualBoundParameters = actualBoundParameters,
            nextParameterIndex = nextParameterIndex,
            dataType = dataType
        )

        val completedSelectStatement =
            selectStatement.replace("%limit%", "${context.size}")
                .replace("%offset%", "${context.page * context.size}")
                .replace("%order%", "${context.sort ?: actualBoundParameters[":order"]!!.value}")

        val sqlSelectStatement = String.format(completedSelectStatement, additionalClauses.toString())
        val selectJob =
            ioCoroutineScope.async { selectRecords(sqlSelectStatement, actualBoundParameters, actualStart, actualEnd) }

        val sqlCountStatement = String.format(countStatement, additionalClauses.toString())
        val countJob =
            ioCoroutineScope.async { countRecords(sqlCountStatement, actualBoundParameters, actualStart, actualEnd) }

        val totalElements = countJob.await().toLong()

        return Page(
            page = context.page,
            totalElements = totalElements,
            totalPages = ceil(totalElements.toDouble() / context.size).toInt(),
            elements = selectJob.await()
        )
    }

    private suspend fun selectRecords(
        sqlStatement: String, boundParameters: Map<String, BoundParameter>, start: Instant, end: Instant
    ) = Flux.usingWhen(
        connectionPool.create(), { connection ->
            Mono.from(connection.createStatement(sqlStatement).also { statement ->
                bindArguments(context.tenant, statement, boundParameters, start, end)
            }.execute()).flatMapMany { result ->
                result.map { row, metadata ->
                    converter.convert(row, metadata)
                }
            }
        }, Connection::close
    ).asFlow().toList(mutableListOf<TimeSeriesRecord>())


    private suspend fun countRecords(
        sqlStatement: String, boundParameters: Map<String, BoundParameter>, start: Instant, end: Instant
    ) = Flux.usingWhen(
        connectionPool.create(), { connection ->
            Mono.from(connection.createStatement(sqlStatement).also { statement ->
                bindArguments(context.tenant, statement, boundParameters, start, end)
            }.execute()).flatMapMany { result ->
                result.map { row, _ ->
                    row.get("count", BigDecimal::class.java)
                }
            }
        }, Connection::close
    ).asFlow().toList(mutableListOf()).first()
}