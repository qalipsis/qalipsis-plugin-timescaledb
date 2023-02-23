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

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.query.AggregationQueryExecutionContext
import io.qalipsis.api.query.DataRetrievalQueryExecutionContext
import io.qalipsis.api.query.Page
import io.qalipsis.api.query.QueryAggregationOperator
import io.qalipsis.api.query.QueryAggregationOperator.MAX
import io.qalipsis.api.query.QueryClause
import io.qalipsis.api.query.QueryClauseOperator
import io.qalipsis.api.query.QueryDescription
import io.qalipsis.api.report.TimeSeriesAggregationResult
import io.qalipsis.api.report.TimeSeriesMeter
import io.qalipsis.api.report.TimeSeriesRecord
import io.qalipsis.plugins.timescaledb.TestUtils.fibonacciFromSize
import io.qalipsis.plugins.timescaledb.dataprovider.AggregationExecutor
import io.qalipsis.plugins.timescaledb.dataprovider.DataProviderConfiguration
import io.qalipsis.plugins.timescaledb.dataprovider.DataRetrievalExecutor
import io.qalipsis.plugins.timescaledb.dataprovider.PreparedQueries
import io.qalipsis.plugins.timescaledb.dataprovider.TimeSeriesMeterRecordConverter
import io.qalipsis.plugins.timescaledb.meter.catadioptre.doPublish
import io.qalipsis.plugins.timescaledb.utils.DbUtils
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.Connection
import jakarta.inject.Inject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.reactive.asFlow
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

@Testcontainers
@MicronautTest(startApplication = false, environments = ["standalone", "timescaledb"], transactional = false)
@Timeout(20, unit = TimeUnit.SECONDS)
internal abstract class AbstractMeterQueryGeneratorIntegrationTest : TestPropertyProvider {

    @Inject
    protected lateinit var meterQueryGenerator: AbstractMeterQueryGenerator

    @Inject
    protected lateinit var meterRegistry: TimescaledbMeterRegistry

    @Inject
    private lateinit var timeSeriesMeterRecordConverter: TimeSeriesMeterRecordConverter

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    protected lateinit var connection: ConnectionPool

    protected val start = Instant.parse("2022-08-01T00:00:00Z")

    protected val timeStep = Duration.ofMillis(500)

    protected lateinit var latestTimestamp: Instant

    abstract val dbPort: Int

    override fun getProperties(): Map<String, String> = mapOf(
        "meters.provider.timescaledb.enabled" to "true",
        "meters.provider.timescaledb.host" to "localhost",
        "meters.provider.timescaledb.port" to "$dbPort",
        "meters.provider.timescaledb.database" to DB_NAME,
        "meters.provider.timescaledb.username" to USERNAME,
        "meters.provider.timescaledb.password" to PASSWORD,
        "meters.provider.timescaledb.schema" to SCHEMA,

        "meters.export.enabled" to "true",
        "meters.export.timescaledb.enabled" to "true",
        "meters.export.timescaledb.host" to "localhost",
        "meters.export.timescaledb.port" to "$dbPort",
        "meters.export.timescaledb.database" to DB_NAME,
        "meters.export.timescaledb.username" to USERNAME,
        "meters.export.timescaledb.password" to PASSWORD,
        "meters.export.timescaledb.schema" to SCHEMA
    )

    private var initialized = false

    @BeforeEach
    fun setUpAll() = testDispatcherProvider.run {
        if (!initialized) {
            connection = DbUtils.createConnectionPool(object : DataProviderConfiguration {
                override val host: String = "localhost"
                override val port: Int = dbPort
                override val database: String = DB_NAME
                override val schema: String = SCHEMA
                override val username: String = USERNAME
                override val password: String = PASSWORD
                override val minSize: Int = 1
                override val maxSize: Int = 2
                override val maxIdleTime: Duration = Duration.ofSeconds(30)
                override val enableSsl: Boolean = false
                override val sslMode: SSLMode = SSLMode.ALLOW
                override val sslRootCert: String? = null
                override val sslCert: String? = null
                override val sslKey: String? = null
            })

            Flux.usingWhen(
                connection.create(),
                { connection -> Mono.from(connection.createStatement("TRUNCATE TABLE meters").execute()) },
                Connection::close
            ).asFlow().count()

            var currentMeterTimestamp = start - timeStep.multipliedBy(2)
            // Meters of name "my-meter-1" for the tenant 1.
            val meters1Tenant1 = fibonacciFromSize(1, 12).flatMap { value ->
                currentMeterTimestamp += timeStep
                listOf(
                    TimescaledbMeter(
                        name = "my-meter-1",
                        tenant = "tenant-1",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-1",
                        tags = """{"value-tag": "$value","tenant-tag":"tenant-1","campaign-tag":"my-campaign-1","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentMeterTimestamp),
                        type = "gauge",
                        value = value.toBigDecimal(),
                    ),
                    TimescaledbMeter(
                        name = "my-meter-1",
                        tenant = "tenant-1",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-2",
                        tags = """{"value-tag": "$value","tenant-tag":"tenant-1","campaign-tag":"my-campaign-1","scenario-tag":"my-scenario-2"}""",
                        timestamp = Timestamp.from(currentMeterTimestamp),
                        type = "gauge",
                        value = value.toBigDecimal(),
                    ),
                    TimescaledbMeter(
                        name = "my-meter-1",
                        tenant = "tenant-1",
                        campaign = "my-campaign-2",
                        scenario = "my-scenario-1",
                        tags = """{"value-tag": "$value","tenant-tag":"tenant-1","campaign-tag":"my-campaign-2","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentMeterTimestamp),
                        type = "gauge",
                        value = value.toBigDecimal(),
                    )
                )
            }
            latestTimestamp = currentMeterTimestamp
            // Meters of name "my-meter-2" for the tenant 1.
            currentMeterTimestamp = start - timeStep.multipliedBy(2)
            val meters2Tenant1 = fibonacciFromSize(5, 12).flatMap { value ->
                currentMeterTimestamp += timeStep
                listOf(
                    TimescaledbMeter(
                        name = "my-meter-2",
                        tenant = "tenant-1",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-1",
                        tags = """{"value-tag": "$value","tenant-tag":"tenant-1","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentMeterTimestamp),
                        type = "gauge",
                        value = value.toBigDecimal(),
                    )
                )
            }
            // Meters of name "my-meter-1" for the tenant 2.
            currentMeterTimestamp = start - timeStep.multipliedBy(2)
            val meters1Tenant2 = fibonacciFromSize(8, 12).flatMap { value ->
                currentMeterTimestamp += timeStep
                listOf(
                    TimescaledbMeter(
                        name = "my-meter-1",
                        tenant = "tenant-2",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-1",
                        tags = """{"value-tag": "$value","tenant-tag":"tenant-2","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentMeterTimestamp),
                        type = "gauge",
                        value = value.toBigDecimal(),
                    )
                )
            }

            // Meters of name "my-meter-1" for the default tenant.
            currentMeterTimestamp = start - timeStep.multipliedBy(2)
            val meters1DefaultTenant = fibonacciFromSize(8, 12).flatMap { value ->
                currentMeterTimestamp += timeStep
                listOf(
                    TimescaledbMeter(
                        name = "my-meter-1",
                        tenant = "default-tenant",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-1",
                        tags = """{"value-tag": "$value","tenant-tag":"default-tenant","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentMeterTimestamp),
                        type = "gauge",
                        value = value.toBigDecimal(),
                    )
                )
            }

            // Meters of name "my-meter-2" for the default tenant.
            currentMeterTimestamp = start - timeStep.multipliedBy(2)
            val meters2DefaultTenant = fibonacciFromSize(5, 12).flatMap { value ->
                currentMeterTimestamp += timeStep
                listOf(
                    TimescaledbMeter(
                        name = "my-meter-2",
                        tenant = "default-tenant",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-1",
                        tags = """{"value-tag": "$value","tenant-tag":"default-tenant","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentMeterTimestamp),
                        type = "gauge",
                        value = value.toBigDecimal(),
                    )
                )
            }

            meterRegistry.doPublish(meters1Tenant1 + meters2Tenant1 + meters1Tenant2 + meters1DefaultTenant + meters2DefaultTenant)
            delay(1000) // Wait for the transaction to be fully committed.
            initialized = true
        }
    }

    @Nested
    inner class DataRetrieval {

        @Test
        internal fun `should fetch no data from empty tenant`() =
            testDispatcherProvider.run {
                // given
                val query = QueryDescription(
                    QueryClause("name", QueryClauseOperator.IS, "my-event-1")
                )
                val queryForTenant1 = meterQueryGenerator.prepareQueries("other-tenant", query)
                val result = executeSelect(
                    coroutineScope = this,
                    query = queryForTenant1,
                    start = Instant.EPOCH,
                    end = start + Duration.ofHours(3)
                )

                // then
                assertThat(result).all {
                    prop(Page<*>::elements).isEmpty()
                    prop(Page<*>::totalElements).isEqualTo(0)
                    prop(Page<*>::totalPages).isEqualTo(0)
                    prop(Page<*>::page).isEqualTo(0)
                }
            }

        @Test
        internal fun `should fetch the values in the expected tenant`() =
            testDispatcherProvider.run {
                // given
                val query = QueryDescription(
                    QueryClause("name", QueryClauseOperator.IS, "my-meter-1")
                )
                val queryForTenant1 = meterQueryGenerator.prepareQueries("tenant-1", query)
                var result = executeSelect(
                    coroutineScope = this,
                    query = queryForTenant1,
                    start = Instant.EPOCH,
                    end = start + Duration.ofHours(3)
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull()
                            prop(TimeSeriesMeter::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                        }
                    }
                }

                // when all the meters "my-meter-2" of tenant-1 are selected.
                val queryForTenant2 = meterQueryGenerator.prepareQueries("tenant-2", query)
                result = executeSelect(
                    coroutineScope = this,
                    query = queryForTenant2,
                    start = Instant.EPOCH,
                    end = start + Duration.ofHours(3)
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull()
                            prop(TimeSeriesMeter::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-2")
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the default tenant`() =
            testDispatcherProvider.run {
                // given
                val query = QueryDescription(
                    QueryClause("name", QueryClauseOperator.IS, "my-meter-1")
                )
                val query1ForDefaultTenant = meterQueryGenerator.prepareQueries(null, query)
                var result = executeSelect(
                    coroutineScope = this,
                    query = query1ForDefaultTenant,
                    start = Instant.EPOCH,
                    end = start + Duration.ofHours(3)
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull()
                            prop(TimeSeriesMeter::tags).isNotNull().key("tenant-tag").isEqualTo("default-tenant")
                        }
                    }
                }

                // when all the meters "my-meter-2" of tenant-1 are selected.
                val query2ForDefaultTenant = meterQueryGenerator.prepareQueries(null, QueryDescription(
                    QueryClause("name", QueryClauseOperator.IS, "my-meter-2")
                ))
                result = executeSelect(
                    coroutineScope = this,
                    query = query2ForDefaultTenant,
                    start = Instant.EPOCH,
                    end = start + Duration.ofHours(3)
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-2")
                            prop(TimeSeriesMeter::value).isNotNull()
                            prop(TimeSeriesMeter::tags).isNotNull().key("tenant-tag").isEqualTo("default-tenant")
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the expected name`() =
            testDispatcherProvider.run {
                // given
                val queryForMeter1 = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-meter-1"))
                )
                var result = executeSelect(
                    coroutineScope = this,
                    query = queryForMeter1,
                    start = Instant.EPOCH,
                    end = start + Duration.ofHours(3)
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull()
                            prop(TimeSeriesMeter::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                        }
                    }
                }

                // when all the meters "my-meter-2" of tenant-1 are selected.
                val queryForMeter2 = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-meter-2"))
                )
                result = executeSelect(
                    coroutineScope = this,
                    query = queryForMeter2,
                    start = Instant.EPOCH,
                    end = start + Duration.ofHours(3)
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-2")
                            prop(TimeSeriesMeter::value).isNotNull()
                            prop(TimeSeriesMeter::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the expected time-range`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-meter-1"))
                )

                // when
                var result = executeSelect(
                    coroutineScope = this,
                    query = query,
                    start = Instant.EPOCH,
                    end = start + Duration.ofMinutes(3)
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull()
                            prop(TimeSeriesMeter::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                        }
                    }
                }

                // when
                result = executeSelect(this, query, start + Duration.ofSeconds(2), start + Duration.ofSeconds(4))

                // then
                assertThat(result.elements).all {
                    hasSize(7)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull()
                            prop(TimeSeriesMeter::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                            prop(TimeSeriesMeter::timestamp).isBetween(
                                start = start + Duration.ofSeconds(2),
                                end = start + Duration.ofSeconds(5) // Rounded to the upper second.
                            )
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the expected tag`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("value-tag", QueryClauseOperator.IS, "21"))
                )
                val result = executeSelect(this, query, start, start + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(2)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::value).isNotNull()
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                                key("value-tag").isEqualTo("21")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the equal numeric value`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(QueryClause("value", QueryClauseOperator.IS, "21"))
                )
                val result = executeSelect(this, query, start, start + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(2)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.isEqualTo(21)
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the not equal numeric value`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-meter-1"),
                        QueryClause("value", QueryClauseOperator.IS_NOT, "21")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(11)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.isNotEqualTo(21)
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the greater values`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-meter-1"),
                        QueryClause("value", QueryClauseOperator.IS_GREATER_THAN, "13")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(6)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.isGreaterThan(13)
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the greater or equal to values`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-meter-1"),
                        QueryClause("value", QueryClauseOperator.IS_GREATER_OR_EQUAL_TO, "13")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(7)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.isGreaterThan(12)
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the lower or equal to values`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-meter-1"),
                        QueryClause("value", QueryClauseOperator.IS_LOWER_OR_EQUAL_TO, "13")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(6)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.isLessThan(14)
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the lower than values`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-meter-1"),
                        QueryClause("value", QueryClauseOperator.IS_LOWER_THAN, "13")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(5)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.isLessThan(14)
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the IN numeric values`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("value", QueryClauseOperator.IS_IN, "13, 21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(4)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.isIn(13, 21)
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with NOT IN numeric values`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("value", QueryClauseOperator.IS_NOT_IN, "13, 21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(20)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.all {
                                isNotEqualTo(13)
                                isNotEqualTo(21)
                            }
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the LIKE string single value`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS_LIKE, "mY_me%"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(24)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the NOT LIKE string single value`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS_NOT_LIKE, "mY_me%-2"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the LIKE string values`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS_LIKE, "mY_me%-1, other"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the NOT LIKE string values`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(QueryClause("name", QueryClauseOperator.IS_NOT_LIKE, "mY_me%-1,mY_me%-2"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).isEmpty()
            }

        @Test
        internal fun `should fetch the values with the IN string values`() =
            testDispatcherProvider.run {
                // given
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS_IN, "my-meter-1, my-meter-2")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(24)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with NOT IN number values`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("value", QueryClauseOperator.IS_NOT_IN, "13, 21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(20)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.all {
                                isNotEqualTo(13)
                                isNotEqualTo(21)
                            }
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the IN string values for a tag`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("value-tag", QueryClauseOperator.IS_IN, "8,21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(4)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.isIn(8, 21)
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the NOT IN string values for a tag`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("value-tag", QueryClauseOperator.IS_NOT_IN, "8,21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(20)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::value).isNotNull().transform { it.toInt() }.isNotIn(8, 21)
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should only fetch the expected number of values from the right page`() =
            testDispatcherProvider.run {
                // given
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-meter-1"))
                )
                var result =
                    executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3), size = 9)

                // then
                assertThat(result).all {
                    prop(Page<TimeSeriesRecord>::totalElements).isEqualTo(12)
                    prop(Page<TimeSeriesRecord>::totalPages).isEqualTo(2)
                    prop(Page<TimeSeriesRecord>::page).isEqualTo(0)
                    prop(Page<TimeSeriesRecord>::elements).all {
                        hasSize(9)
                        each {
                            it.isInstanceOf(TimeSeriesMeter::class).all {
                                prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                                prop(TimeSeriesMeter::tags).isNotNull().all {
                                    key("tenant-tag").isEqualTo("tenant-1")
                                }
                            }
                        }
                    }
                }

                // when
                result = executeSelect(
                    this,
                    query,
                    Instant.EPOCH,
                    latestTimestamp + Duration.ofSeconds(3),
                    size = 9,
                    page = 1
                )

                // then
                assertThat(result).all {
                    prop(Page<TimeSeriesRecord>::totalElements).isEqualTo(12)
                    prop(Page<TimeSeriesRecord>::totalPages).isEqualTo(2)
                    prop(Page<TimeSeriesRecord>::page).isEqualTo(1)
                    prop(Page<TimeSeriesRecord>::elements).all {
                        hasSize(3)
                        each {
                            it.isInstanceOf(TimeSeriesMeter::class).all {
                                prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                                prop(TimeSeriesMeter::tags).isNotNull().all {
                                    key("tenant-tag").isEqualTo("tenant-1")
                                }
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the descending order by default`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-meter-1"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                var previousTimestamp: Instant = Instant.MAX
                var currentTimestamp: Instant = Instant.MAX
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::timestamp).isNotNull()
                                .transform { it.also { currentTimestamp = it } }
                                .isLessThan(previousTimestamp)
                        }
                        previousTimestamp = currentTimestamp
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the ascending order when specified`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-meter-1"))
                )
                val result = executeSelect(
                    coroutineScope = this,
                    query = query,
                    start = Instant.EPOCH,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    order = "asc"
                )

                // then
                var previousTimestamp: Instant = Instant.EPOCH
                var currentTimestamp: Instant = Instant.EPOCH
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::timestamp).isNotNull()
                                .transform { it.also { currentTimestamp = it } }
                                .isGreaterThan(previousTimestamp)
                        }
                        previousTimestamp = currentTimestamp
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the ascending order when set`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-meter-1"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                var previousTimestamp: Instant = Instant.MAX
                var currentTimestamp: Instant = Instant.MAX
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::timestamp).isNotNull()
                                .transform { it.also { currentTimestamp = it } }
                                .isLessThan(previousTimestamp)
                        }
                        previousTimestamp = currentTimestamp
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the right campaigns`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries("tenant-1", QueryDescription())
                var result = executeSelect(
                    coroutineScope = this,
                    query = query,
                    start = Instant.EPOCH,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1")
                )

                // then
                assertThat(result.elements).all {
                    hasSize(24)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::campaign).isEqualTo("my-campaign-1")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }

                // when
                result = executeSelect(
                    coroutineScope = this,
                    query = query,
                    start = Instant.EPOCH,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-2")
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::campaign).isEqualTo("my-campaign-2")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }

                // when
                result = executeSelect(
                    coroutineScope = this,
                    query = query,
                    start = Instant.EPOCH,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1", "my-campaign-2")
                )

                // then
                assertThat(result.elements).all {
                    hasSize(36)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::campaign).isIn("my-campaign-1", "my-campaign-2")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the right campaigns and scenarios`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries("tenant-1", QueryDescription())
                var result = executeSelect(
                    coroutineScope = this,
                    query = query,
                    start = Instant.EPOCH,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1"),
                    scenariosNames = setOf("my-scenario-1")
                )

                // then
                assertThat(result.elements).all {
                    hasSize(24)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::campaign).isEqualTo("my-campaign-1")
                            prop(TimeSeriesMeter::scenario).isEqualTo("my-scenario-1")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }

                // when
                result = executeSelect(
                    coroutineScope = this,
                    query = query,
                    start = Instant.EPOCH,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1", "my-campaign-2"),
                    scenariosNames = setOf("my-scenario-1")
                )

                // then
                assertThat(result.elements).all {
                    hasSize(36)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isIn("my-meter-1", "my-meter-2")
                            prop(TimeSeriesMeter::campaign).isIn("my-campaign-1", "my-campaign-2")
                            prop(TimeSeriesMeter::scenario).isEqualTo("my-scenario-1")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }

                // when
                result = executeSelect(
                    coroutineScope = this,
                    query = query,
                    start = Instant.EPOCH,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1", "my-campaign-2"),
                    scenariosNames = setOf("my-scenario-2")
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesMeter::class).all {
                            prop(TimeSeriesMeter::name).isEqualTo("my-meter-1")
                            prop(TimeSeriesMeter::campaign).isIn("my-campaign-1", "my-campaign-2")
                            prop(TimeSeriesMeter::scenario).isEqualTo("my-scenario-2")
                            prop(TimeSeriesMeter::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }
    }

    @Nested
    open inner class AggregationCalculation {

        @Test
        internal fun `should not generate the query when the field is not known`() =
            testDispatcherProvider.run {
                // given
                val aggregationQuery = QueryDescription(
                    fieldName = "unknown-field",
                    QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                )

                // then
                val exception = assertThrows<IllegalArgumentException> {
                    meterQueryGenerator.prepareQueries(
                        "tenant-1",
                        aggregationQuery
                    )
                }
                assertThat(exception.message).isEqualTo("The field unknown-field is not valid for a data series of type METER")
            }

        @Test
        internal fun `should not generate the query when aggregating a non number field`() =
            testDispatcherProvider.run {
                // given
                val aggregationQuery = QueryDescription(
                    fieldName = "other",
                    aggregationOperation = MAX
                )

                // then
                val exception = assertThrows<IllegalArgumentException> {
                    meterQueryGenerator.prepareQueries(
                        "tenant-1",
                        aggregationQuery
                    )
                }
                assertThat(exception.message).isEqualTo("The field other is not numeric and cannot be aggregated")
            }

        @Test
        internal fun `should aggregate no data from empty tenant`() =
            testDispatcherProvider.run {
                // given
                val aggregationQuery = QueryDescription(
                    QueryClause("name", QueryClauseOperator.IS, "my-event-1")
                )
                val query = meterQueryGenerator.prepareQueries("other-tenant", aggregationQuery)
                val result = executeAggregation(query, start, latestTimestamp - timeStep)

                // then
                assertThat(result).isEmpty()
            }

        @Test
        internal fun `should calculate the average of the number values of the expected meter name, tenant and time range`() =
            testDispatcherProvider.run {
                // given
                val queryForMeter1InTenant1 = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                        fieldName = "value",
                        aggregationOperation = QueryAggregationOperator.AVERAGE,
                        timeframeUnit = Duration.ofSeconds(2)
                    )
                )
                var result = executeAggregation(queryForMeter1InTenant1, start, latestTimestamp - timeStep)

                // then
                assertThat(result).all {
                    hasSize(3)
                    index(0).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.5)
                    }
                    index(1).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                            .isEqualTo(30.75)
                    }
                    index(2).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                            .isEqualTo(155.0 + 1.0 / 3)
                    }
                }

                // when all the meters "my-meter-1" of tenant-1 are selected.
                result = executeAggregation(
                    queryForMeter1InTenant1,
                    start - Duration.ofMinutes(1),
                    latestTimestamp + Duration.ofMinutes(1)
                )

                // then
                assertThat(result).all {
                    hasSize(4)
                    index(0).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start - Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(1.0)
                    }
                    index(1).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.5)
                    }
                    index(2).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                            .isEqualTo(30.75)
                    }
                    index(3).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                            .isEqualTo(155.0 + 1.0 / 3)
                    }
                }

                // when all the meters "my-meter-2" of tenant-1 are selected.
                val queryForMeter2InTenant1 = meterQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-2")),
                        fieldName = "value",
                        aggregationOperation = QueryAggregationOperator.AVERAGE,
                        timeframeUnit = Duration.ofSeconds(2)
                    )
                )
                result = executeAggregation(queryForMeter2InTenant1, start, latestTimestamp - timeStep)

                // then
                assertThat(result).all {
                    hasSize(3)
                    index(0).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start.truncatedTo(ChronoUnit.SECONDS))
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                            .isEqualTo(11.75)
                    }
                    index(1).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(
                            start.truncatedTo(ChronoUnit.SECONDS) + Duration.ofSeconds(
                                2
                            )
                        )
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(80.5)
                    }
                    index(2).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(
                            start.truncatedTo(ChronoUnit.SECONDS) + Duration.ofSeconds(
                                4
                            )
                        )
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                            .isEqualTo(406.0 + (2.0 / 3))
                    }
                }
            }

        @Test
        internal fun `should calculate the count of all records`() = testDispatcherProvider.run {
            // given
            val query = meterQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                    fieldName = null,
                    aggregationOperation = QueryAggregationOperator.COUNT,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                }
                index(1).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                }
                index(2).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(3.0)
                }
            }
        }

        @Test
        internal fun `should calculate the count of non-null values`() = testDispatcherProvider.run {
            // given
            val query = meterQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                    fieldName = "value",
                    aggregationOperation = QueryAggregationOperator.COUNT,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                }
                index(1).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                }
                index(2).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(3.0)
                }
            }
        }

        @Test
        internal fun `should calculate the min values`() = testDispatcherProvider.run {
            // given
            val query = meterQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                    fieldName = "value",
                    aggregationOperation = QueryAggregationOperator.MIN,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(2.0)
                }
                index(1).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(13.0)
                }
                index(2).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(89.0)
                }
            }
        }

        @Test
        internal fun `should calculate the max values`() = testDispatcherProvider.run {
            // given
            val query = meterQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                    fieldName = "value",
                    aggregationOperation = QueryAggregationOperator.MAX,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                }
                index(1).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(55.0)
                }
                index(2).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(233.0)
                }
            }
        }

        @Test
        internal fun `should calculate the sum values`() = testDispatcherProvider.run {
            // given
            val query = meterQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                    fieldName = "value",
                    aggregationOperation = QueryAggregationOperator.SUM,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(18.0)
                }
                index(1).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(123.0)
                }
                index(2).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(466.0)
                }
            }
        }

        @Test
        internal fun `should calculate the standard deviation values`() = testDispatcherProvider.run {
            // given
            val query = meterQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                    fieldName = "value",
                    aggregationOperation = QueryAggregationOperator.STANDARD_DEVIATION,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                        .isStrictlyBetween(2.64, 2.65)
                }
                index(1).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                        .isStrictlyBetween(18.33, 18.34)
                }
                index(2).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                        .isStrictlyBetween(72.66, 72.67)
                }
            }
        }

        @Test
        internal fun `should calculate the 99% percentile values`() = testDispatcherProvider.run {
            // given
            val query = meterQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                    fieldName = "value",
                    aggregationOperation = QueryAggregationOperator.PERCENTILE_99,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                }
                index(1).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(55.0)
                }
                index(2).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(233.0)
                }
            }
        }

        @Test
        internal fun `should calculate the 99 9% percentile values`() = testDispatcherProvider.run {
            // given
            val query = meterQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                    fieldName = "value",
                    aggregationOperation = QueryAggregationOperator.PERCENTILE_99_9,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                }
                index(1).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(55.0)
                }
                index(2).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(233.0)
                }
            }
        }

        @Test
        internal fun `should aggregate in the right campaigns`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(aggregationOperation = QueryAggregationOperator.COUNT)
                )
                var result = executeAggregation(
                    query = query,
                    start = start,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1")
                )

                // then
                assertThat(result).all {
                    hasSize(3)
                    index(0).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                    }
                    index(1).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                    }
                    index(2).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(6.0)
                    }
                }

                // when
                result = executeAggregation(
                    query = query,
                    start = start,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-2")
                )

                // then
                assertThat(result).all {
                    hasSize(3)
                    index(0).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-2")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                    }
                    index(1).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-2")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                    }
                    index(2).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-2")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(3.0)
                    }
                }

                // when
                result = executeAggregation(
                    query = query,
                    start = start,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1", "my-campaign-2")
                )

                // then
                assertThat(result).all {
                    hasSize(6)
                    index(0).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                    }
                    index(1).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                    }
                    index(2).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(6.0)
                    }
                    index(3).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-2")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                    }
                    index(4).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-2")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                    }
                    index(5).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-2")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(3.0)
                    }
                }
            }

        @Test
        internal fun `should aggregate in the right campaigns and scenarios`() =
            testDispatcherProvider.run {
                // given"
                val query = meterQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(aggregationOperation = QueryAggregationOperator.COUNT)
                )
                var result = executeAggregation(
                    query = query,
                    start = start,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1"),
                    scenariosNames = setOf("my-scenario-1")
                )

                // then
                assertThat(result).all {
                    hasSize(3)
                    index(0).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                    }
                    index(1).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                    }
                    index(2).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(6.0)
                    }
                }

                // when
                result = executeAggregation(
                    query = query,
                    start = start,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1", "my-campaign-2"),
                    scenariosNames = setOf("my-scenario-1")
                )

                // then
                assertThat(result).all {
                    hasSize(6)
                    index(0).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                    }
                    index(1).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(8.0)
                    }
                    index(2).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(6.0)
                    }
                    index(3).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-2")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                    }
                    index(4).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-2")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                    }
                    index(5).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-2")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(3.0)
                    }
                }

                // when
                result = executeAggregation(
                    query = query,
                    start = start,
                    end = latestTimestamp + Duration.ofSeconds(3),
                    campaigns = setOf("my-campaign-1", "my-campaign-2"),
                    scenariosNames = setOf("my-scenario-2")
                )

                // then
                assertThat(result).all {
                    hasSize(3)
                    index(0).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                    }
                    index(1).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(4.0)
                    }
                    index(2).all {
                        prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                        prop(TimeSeriesAggregationResult::campaign).isEqualTo("my-campaign-1")
                        prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }.isEqualTo(3.0)
                    }
                }
            }
    }

    protected open suspend fun executeSelect(
        coroutineScope: CoroutineScope,
        query: PreparedQueries,
        start: Instant,
        end: Instant,
        timeframe: Duration = Duration.ofSeconds(1),
        page: Int = 0,
        size: Int = 100,
        order: String? = null,
        campaigns: Set<String> = setOf("my-campaign-1"),
        scenariosNames: Set<String> = setOf("my-scenario-1")
    ): Page<TimeSeriesRecord> {
        return DataRetrievalExecutor(
            coroutineScope,
            timeSeriesMeterRecordConverter,
            connection,
            DataRetrievalQueryExecutionContext(
                tenant = "default-tenant",
                campaignsReferences = campaigns,
                scenariosNames = scenariosNames,
                from = start,
                until = end,
                aggregationTimeframe = timeframe,
                size = size,
                page = page,
                sort = order
            ),
            query.countStatement,
            query.retrievalStatement,
            query.retrievalBoundParameters,
            query.nextAvailableRetrievalParameterIdentifierIndex
        ).execute()
    }

    protected open suspend fun executeAggregation(
        query: PreparedQueries,
        start: Instant,
        end: Instant,
        timeframe: Duration = Duration.ofSeconds(2),
        campaigns: Set<String> = setOf("my-campaign-1"),
        scenariosNames: Set<String> = setOf("my-scenario-1")
    ): List<TimeSeriesAggregationResult> {
        return AggregationExecutor(
            connection,
            AggregationQueryExecutionContext(
                tenant = "default-tenant",
                campaignsReferences = campaigns,
                scenariosNames = scenariosNames,
                from = start,
                until = end,
                aggregationTimeframe = timeframe
            ),
            query.aggregationStatement,
            query.aggregationBoundParameters,
            query.nextAvailableAggregationParameterIdentifierIndex
        ).execute()
    }

    companion object {

        val log = logger()

        /**
         * Default db name.
         */
        const val DB_NAME = "qalipsis"

        /**
         * Default username.
         */
        const val USERNAME = "qalipsis_user"

        /**
         * Default password.
         */
        const val PASSWORD = "qalipsis-pwd"

        /**
         * Default schema.
         */
        const val SCHEMA = "meters"

    }
}