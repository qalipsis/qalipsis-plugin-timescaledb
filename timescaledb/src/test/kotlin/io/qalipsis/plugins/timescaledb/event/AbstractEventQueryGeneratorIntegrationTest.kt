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

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.query.AggregationQueryExecutionContext
import io.qalipsis.api.query.DataRetrievalQueryExecutionContext
import io.qalipsis.api.query.Page
import io.qalipsis.api.query.QueryAggregationOperator
import io.qalipsis.api.query.QueryClause
import io.qalipsis.api.query.QueryClauseOperator
import io.qalipsis.api.query.QueryDescription
import io.qalipsis.api.report.TimeSeriesAggregationResult
import io.qalipsis.api.report.TimeSeriesEvent
import io.qalipsis.api.report.TimeSeriesRecord
import io.qalipsis.plugins.timescaledb.TestUtils.fibonacciFromSize
import io.qalipsis.plugins.timescaledb.dataprovider.AggregationExecutor
import io.qalipsis.plugins.timescaledb.dataprovider.DataProviderConfiguration
import io.qalipsis.plugins.timescaledb.dataprovider.DataRetrievalExecutor
import io.qalipsis.plugins.timescaledb.dataprovider.PreparedQueries
import io.qalipsis.plugins.timescaledb.dataprovider.TimeSeriesEventRecordConverter
import io.qalipsis.plugins.timescaledb.event.catadioptre.publishConvertedEvents
import io.qalipsis.plugins.timescaledb.utils.DbUtils
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.spi.Connection
import jakarta.inject.Inject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.reactive.asFlow
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
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
@MicronautTest(startApplication = false, environments = ["standalone"])
@Timeout(20, unit = TimeUnit.SECONDS)
internal abstract class AbstractEventQueryGeneratorIntegrationTest : TestPropertyProvider {

    @Inject
    protected lateinit var eventsConverter: TimescaledbEventConverter

    @Inject
    protected lateinit var eventQueryGenerator: AbstractEventQueryGenerator

    @Inject
    private lateinit var timeSeriesEventRecordConverter: TimeSeriesEventRecordConverter

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    protected lateinit var connection: ConnectionPool

    protected lateinit var publisher: TimescaledbEventsPublisher

    protected val start: Instant = Instant.parse("2022-08-01T00:00:00Z")

    protected val timeStep: Duration = Duration.ofMillis(500)

    protected lateinit var latestTimestamp: Instant

    abstract val dbPort: Int

    override fun getProperties(): Map<String, String> = mapOf(
        "events.provider.timescaledb.enabled" to "true",
        "events.provider.timescaledb.host" to "localhost",
        "events.provider.timescaledb.port" to "$dbPort",
        "events.provider.timescaledb.database" to DB_NAME,
        "events.provider.timescaledb.username" to USERNAME,
        "events.provider.timescaledb.password" to PASSWORD,
        "events.provider.timescaledb.schema" to SCHEMA
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

            })

            Flux.usingWhen(
                connection.create(),
                { connection -> Mono.from(connection.createStatement("TRUNCATE TABLE events").execute()) },
                Connection::close
            ).asFlow().count()

            val configuration = mockk<TimescaledbEventsPublisherConfiguration> {
                every { host } returns "localhost"
                every { port } returns dbPort
                every { database } returns DB_NAME
                every { schema } returns SCHEMA
                every { username } returns USERNAME
                every { password } returns PASSWORD
                every { minLevel } returns EventLevel.TRACE
                every { lingerPeriod } returns Duration.ofNanos(1)
                every { batchSize } returns 2000
                every { publishers } returns 1
            }

            publisher =
                TimescaledbEventsPublisher(
                    CoroutineScope(SupervisorJob() + Dispatchers.IO),
                    configuration,
                    eventsConverter
                )
            publisher.start()

            var currentEventTimestamp = start - timeStep.multipliedBy(2)
            // Events of name "my-event-1" for the tenant 1.
            val events1Tenant1 = fibonacciFromSize(1, 12).flatMap { number ->
                currentEventTimestamp += timeStep
                listOf(
                    TimescaledbEvent(
                        name = "my-event-1",
                        level = "info",
                        tenant = "tenant-1",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-1",
                        tags = """{"number-tag": "$number","tenant-tag":"tenant-1","campaign-tag":"my-campaign-1","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentEventTimestamp),
                        number = number.toBigDecimal(),
                    ),
                    TimescaledbEvent(
                        name = "my-event-1",
                        level = "info",
                        tenant = "tenant-1",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-2",
                        tags = """{"number-tag": "$number","tenant-tag":"tenant-1","campaign-tag":"my-campaign-1","scenario-tag":"my-scenario-2"}""",
                        timestamp = Timestamp.from(currentEventTimestamp),
                        number = number.toBigDecimal(),
                    ),
                    TimescaledbEvent(
                        name = "my-event-1",
                        level = "info",
                        tenant = "tenant-1",
                        campaign = "my-campaign-2",
                        scenario = "my-scenario-1",
                        tags = """{"number-tag": "$number","tenant-tag":"tenant-1","campaign-tag":"my-campaign-2","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentEventTimestamp),
                        number = number.toBigDecimal(),
                    )
                )
            }
            latestTimestamp = currentEventTimestamp
            // Events of name "my-event-2" for the tenant 1.
            currentEventTimestamp = start - timeStep.multipliedBy(2)
            val events2Tenant1 = fibonacciFromSize(5, 12).flatMap { number ->
                currentEventTimestamp += timeStep
                listOf(
                    TimescaledbEvent(
                        name = "my-event-2",
                        level = "info",
                        tenant = "tenant-1",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-1",
                        tags = """{"number-tag": "$number","tenant-tag":"tenant-1","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentEventTimestamp),
                        number = number.toBigDecimal(),
                    )
                )
            }
            // Events of name "my-event-1" for the tenant 2.
            currentEventTimestamp = start - timeStep.multipliedBy(2)
            val events1Tenant2 = fibonacciFromSize(8, 12).flatMap { number ->
                currentEventTimestamp += timeStep
                listOf(
                    TimescaledbEvent(
                        name = "my-event-1",
                        level = "info",
                        tenant = "tenant-2",
                        campaign = "my-campaign-1",
                        scenario = "my-scenario-1",
                        tags = """{"number-tag": "$number","tenant-tag":"tenant-2","scenario-tag":"my-scenario-1"}""",
                        timestamp = Timestamp.from(currentEventTimestamp),
                        number = number.toBigDecimal(),
                    )
                )
            }

            publisher.publishConvertedEvents(events1Tenant1 + events2Tenant1 + events1Tenant2)
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
                val queryForTenant1 = eventQueryGenerator.prepareQueries("other-tenant", query)
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
        internal fun `should fetch the numbers in the expected tenant`() =
            testDispatcherProvider.run {
                // given
                val query = QueryDescription(
                    QueryClause("name", QueryClauseOperator.IS, "my-event-1")
                )
                val queryForTenant1 = eventQueryGenerator.prepareQueries("tenant-1", query)
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull()
                            prop(TimeSeriesEvent::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                        }
                    }
                }

                // when all the events "my-event-2" of tenant-1 are selected.
                val queryForTenant2 = eventQueryGenerator.prepareQueries("tenant-2", query)
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull()
                            prop(TimeSeriesEvent::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-2")
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers in the expected name`() =
            testDispatcherProvider.run {
                // given
                val queryForEvent1 = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-event-1"))
                )
                var result = executeSelect(
                    coroutineScope = this,
                    query = queryForEvent1,
                    start = Instant.EPOCH,
                    end = start + Duration.ofHours(3)
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull()
                            prop(TimeSeriesEvent::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                        }
                    }
                }

                // when all the events "my-event-2" of tenant-1 are selected.
                val queryForEvent2 = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-event-2"))
                )
                result = executeSelect(
                    coroutineScope = this,
                    query = queryForEvent2,
                    start = Instant.EPOCH,
                    end = start + Duration.ofHours(3)
                )

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-2")
                            prop(TimeSeriesEvent::number).isNotNull()
                            prop(TimeSeriesEvent::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers in the expected time-range`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-event-1"))
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull()
                            prop(TimeSeriesEvent::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                        }
                    }
                }

                // when
                result = executeSelect(this, query, start + Duration.ofSeconds(2), start + Duration.ofSeconds(4))

                // then
                assertThat(result.elements).all {
                    hasSize(7)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull()
                            prop(TimeSeriesEvent::tags).isNotNull().key("tenant-tag").isEqualTo("tenant-1")
                            prop(TimeSeriesEvent::timestamp).isBetween(
                                start = start + Duration.ofSeconds(2),
                                end = start + Duration.ofSeconds(5) // Rounded to the upper second.
                            )
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the expected tag`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("number-tag", QueryClauseOperator.IS, "21"))
                )
                val result = executeSelect(this, query, start, start + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(2)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::number).isNotNull()
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                                key("number-tag").isEqualTo("21")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the equal numeric number`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(QueryClause("number", QueryClauseOperator.IS, "21"))
                )
                val result = executeSelect(this, query, start, start + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(2)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.isEqualTo(21)
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the not equal numeric number`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                        QueryClause("number", QueryClauseOperator.IS_NOT, "21")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(11)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.isNotEqualTo(21)
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the greater numbers`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                        QueryClause("number", QueryClauseOperator.IS_GREATER_THAN, "13")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(6)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.isGreaterThan(13)
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the greater or equal to numbers`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                        QueryClause("number", QueryClauseOperator.IS_GREATER_OR_EQUAL_TO, "13")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(7)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.isGreaterThan(12)
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the lower or equal to numbers`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                        QueryClause("number", QueryClauseOperator.IS_LOWER_OR_EQUAL_TO, "13")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(6)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.isLessThan(14)
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the lower than numbers`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                        QueryClause("number", QueryClauseOperator.IS_LOWER_THAN, "13")
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(5)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.isLessThan(14)
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the IN numeric numbers`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("number", QueryClauseOperator.IS_IN, "13, 21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(4)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.isIn(13, 21)
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with NOT IN numeric numbers`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("number", QueryClauseOperator.IS_NOT_IN, "13, 21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(20)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.all {
                                isNotEqualTo(13)
                                isNotEqualTo(21)
                            }
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the LIKE string single number`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS_LIKE, "mY_ev%"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(24)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the NOT LIKE string single number`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS_NOT_LIKE, "mY_ev%-2"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the LIKE string numbers`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS_LIKE, "mY_ev%-1, other"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the NOT LIKE string numbers`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(QueryClause("name", QueryClauseOperator.IS_NOT_LIKE, "mY_ev%-1,mY_ev%-2"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).isEmpty()
            }

        @Test
        internal fun `should fetch the numbers with the IN string numbers`() =
            testDispatcherProvider.run {
                // given
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS_IN, "my-event-1, my-event-2")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(24)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with NOT IN number numbers`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("number", QueryClauseOperator.IS_NOT_IN, "13, 21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(20)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.all {
                                isNotEqualTo(13)
                                isNotEqualTo(21)
                            }
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the IN string numbers for a tag`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("number-tag", QueryClauseOperator.IS_IN, "8,21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(4)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.isIn(8, 21)
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers with the NOT IN string numbers for a tag`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("number-tag", QueryClauseOperator.IS_NOT_IN, "8,21"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result.elements).all {
                    hasSize(20)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::number).isNotNull().transform { it.toInt() }.isNotIn(8, 21)
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should only fetch the expected number of numbers from the right page`() =
            testDispatcherProvider.run {
                // given
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1",
                    QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-event-1"))
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
                            it.isInstanceOf(TimeSeriesEvent::class).all {
                                prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                                prop(TimeSeriesEvent::tags).isNotNull().all {
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
                            it.isInstanceOf(TimeSeriesEvent::class).all {
                                prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                                prop(TimeSeriesEvent::tags).isNotNull().all {
                                    key("tenant-tag").isEqualTo("tenant-1")
                                }
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers in the descending order by default`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-event-1"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                var previousTimestamp: Instant = Instant.MAX
                var currentTimestamp: Instant = Instant.MAX
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::timestamp).isNotNull()
                                .transform { it.also { currentTimestamp = it } }
                                .isLessThan(previousTimestamp)
                        }
                        previousTimestamp = currentTimestamp
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers in the ascending order when specified`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-event-1"))
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::timestamp).isNotNull()
                                .transform { it.also { currentTimestamp = it } }
                                .isGreaterThan(previousTimestamp)
                        }
                        previousTimestamp = currentTimestamp
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers in the ascending order when set`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(QueryClause("name", QueryClauseOperator.IS, "my-event-1"))
                )
                val result = executeSelect(this, query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                var previousTimestamp: Instant = Instant.MAX
                var currentTimestamp: Instant = Instant.MAX
                assertThat(result.elements).all {
                    hasSize(12)
                    each {
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::timestamp).isNotNull()
                                .transform { it.also { currentTimestamp = it } }
                                .isLessThan(previousTimestamp)
                        }
                        previousTimestamp = currentTimestamp
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers in the right campaigns`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries("tenant-1", QueryDescription())
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::campaign).isEqualTo("my-campaign-1")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::campaign).isEqualTo("my-campaign-2")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::campaign).isIn("my-campaign-1", "my-campaign-2")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
                                key("tenant-tag").isEqualTo("tenant-1")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the numbers in the right campaigns and scenarios`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries("tenant-1", QueryDescription())
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::campaign).isEqualTo("my-campaign-1")
                            prop(TimeSeriesEvent::scenario).isEqualTo("my-scenario-1")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isIn("my-event-1", "my-event-2")
                            prop(TimeSeriesEvent::campaign).isIn("my-campaign-1", "my-campaign-2")
                            prop(TimeSeriesEvent::scenario).isEqualTo("my-scenario-1")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
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
                        it.isInstanceOf(TimeSeriesEvent::class).all {
                            prop(TimeSeriesEvent::name).isEqualTo("my-event-1")
                            prop(TimeSeriesEvent::campaign).isIn("my-campaign-1", "my-campaign-2")
                            prop(TimeSeriesEvent::scenario).isEqualTo("my-scenario-2")
                            prop(TimeSeriesEvent::tags).isNotNull().all {
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
        internal fun `should aggregate no data from empty tenant`() =
            testDispatcherProvider.run {
                // given
                val aggregationQuery = QueryDescription(
                    QueryClause("name", QueryClauseOperator.IS, "my-event-1")
                )
                val query = eventQueryGenerator.prepareQueries("other-tenant", aggregationQuery)
                val result = executeAggregation(query, start, latestTimestamp - timeStep)

                // then
                assertThat(result).isEmpty()
            }

        @Test
        internal fun `should calculate the average of the number values of the expected event name, tenant and time range`() =
            testDispatcherProvider.run {
                // given
                val queryForEvent1InTenant1 = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                        fieldName = "number",
                        aggregationOperation = QueryAggregationOperator.AVERAGE,
                        timeframeUnit = Duration.ofSeconds(2)
                    )
                )
                var result = executeAggregation(queryForEvent1InTenant1, start, latestTimestamp - timeStep)

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

                // when all the events "my-event-1" of tenant-1 are selected.
                result = executeAggregation(
                    queryForEvent1InTenant1,
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

                // when all the events "my-event-2" of tenant-1 are selected.
                val queryForEvent2InTenant1 = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-2")),
                        fieldName = "number",
                        aggregationOperation = QueryAggregationOperator.AVERAGE,
                        timeframeUnit = Duration.ofSeconds(2)
                    )
                )
                result = executeAggregation(queryForEvent2InTenant1, start, latestTimestamp - timeStep)

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
            val query = eventQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
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
            val query = eventQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                    fieldName = "number",
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
            val query = eventQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                    fieldName = "number",
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
            val query = eventQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                    fieldName = "number",
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
            val query = eventQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                    fieldName = "number",
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
            val query = eventQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                    fieldName = "number",
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
            val query = eventQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                    fieldName = "number",
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
        internal fun `should aggregate in the right campaigns`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
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
                val query = eventQueryGenerator.prepareQueries(
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
            timeSeriesEventRecordConverter,
            connection,
            DataRetrievalQueryExecutionContext(
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
        const val SCHEMA = "events"

    }
}