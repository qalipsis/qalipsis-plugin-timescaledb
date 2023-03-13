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

import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.lang.Nullable
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.query.AggregationQueryExecutionContext
import io.qalipsis.api.query.DataRetrievalQueryExecutionContext
import io.qalipsis.api.query.Page
import io.qalipsis.api.report.TimeSeriesAggregationResult
import io.qalipsis.api.report.TimeSeriesDataProvider
import io.qalipsis.api.report.TimeSeriesRecord
import io.qalipsis.plugins.timescaledb.event.TimescaledbEventDataProviderConfiguration
import io.qalipsis.plugins.timescaledb.meter.TimescaledbMeterDataProviderConfiguration
import io.qalipsis.plugins.timescaledb.utils.DbUtils
import io.r2dbc.pool.ConnectionPool
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout

@Singleton
@Requires(bean = AbstractDataProvider::class)
internal class TimescaledbTimeSeriesDataProvider(
    private val objectMapper: ObjectMapper,
    @Nullable @Named("event-data-provider") private val eventConnectionPool: ConnectionPool?,
    @Nullable @Named("meter-data-provider") private val meterConnectionPool: ConnectionPool?,
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineScope: CoroutineScope,
    private val timeSeriesMeterRecordConverter: TimeSeriesMeterRecordConverter,
    private val timeSeriesEventRecordConverter: TimeSeriesEventRecordConverter,
    @Nullable private val timescaledbEventDataProviderConfiguration: TimescaledbEventDataProviderConfiguration?,
    @Nullable private val timescaledbMeterDataProviderConfiguration: TimescaledbMeterDataProviderConfiguration?
) : TimeSeriesDataProvider {

    override suspend fun executeAggregations(
        preparedQueries: Map<String, String>,
        context: AggregationQueryExecutionContext
    ): Map<String, List<TimeSeriesAggregationResult>> {
        return prepareAndExecuteConcurrentQueries(preparedQueries) { connectionPool, query ->
            buildAggregationExecutor(
                connectionPool,
                context,
                query
            )
        }
    }

    private fun buildAggregationExecutor(
        connectionPool: ConnectionPool,
        context: AggregationQueryExecutionContext,
        query: PreparedQueries
    ) = AggregationExecutor(
        connectionPool,
        context,
        query.aggregationStatement,
        query.aggregationBoundParameters,
        query.nextAvailableAggregationParameterIdentifierIndex,
        query.dataType
    )

    override suspend fun retrieveRecords(
        preparedQueries: Map<String, String>,
        context: DataRetrievalQueryExecutionContext
    ): Map<String, Page<TimeSeriesRecord>> {
        return prepareAndExecuteConcurrentQueries(preparedQueries) { connectionPool, query ->
            buildRetrievalExecutor(
                connectionPool,
                context,
                query
            )
        }
    }

    override suspend fun retrieveUsedStorage(tenant: String): Long {
        val eventsStorage = eventConnectionPool?.let { connection ->
            DbUtils.fetchStorage(
                connection,
                tenant,
                timescaledbEventDataProviderConfiguration?.schema!!,
                "events"
            )
        } ?: 0
        val metersStorage = meterConnectionPool?.let { connection ->
            DbUtils.fetchStorage(
                connection,
                tenant,
                timescaledbMeterDataProviderConfiguration?.schema!!,
                "meters"
            )
        } ?: 0

        return eventsStorage + metersStorage
    }

    private fun buildRetrievalExecutor(
        connectionPool: ConnectionPool,
        context: DataRetrievalQueryExecutionContext,
        query: PreparedQueries
    ) = DataRetrievalExecutor(
        ioCoroutineScope,
        when (query.dataType) {
            DataType.METER -> timeSeriesMeterRecordConverter
            DataType.EVENT -> timeSeriesEventRecordConverter
        },
        connectionPool,
        context,
        query.countStatement,
        query.retrievalStatement,
        query.retrievalBoundParameters,
        query.nextAvailableRetrievalParameterIdentifierIndex,
        query.dataType
    )

    private suspend fun <T> prepareAndExecuteConcurrentQueries(
        preparedQueries: Map<String, String>,
        executorBuilder: (connectionPool: ConnectionPool, query: PreparedQueries) -> AbstractQueryExecutor<T>
    ): Map<String, T> {
        val keyedJobs = preparedQueries
            .mapValues {
                val query = objectMapper.readValue(it.value, PreparedQueries::class.java)
                val connectionPool = when (query.dataType) {
                    DataType.METER -> requireNotNull(meterConnectionPool) { "The TimescaleDB meters provider was not enabled" }
                    DataType.EVENT -> requireNotNull(eventConnectionPool) { "The TimescaleDB events provider was not enabled" }
                }
                val executor = executorBuilder(connectionPool, query)
                ioCoroutineScope.async {
                    withTimeout(Duration.ofSeconds(20).toMillis()) {
                        executor.execute()
                    }
                }
            }.toList()

        return flowOf(*keyedJobs.toTypedArray())
            .mapNotNull { (key, job) ->
                try {
                    key to job.await()
                } catch (e: Exception) {
                    log.error(e) { "" }
                    null
                }
            }.toList(mutableListOf())
            .toMap()
    }

    private companion object {

        val log = logger()
    }
}