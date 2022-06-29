package io.qalipsis.plugins.timescaledb.event

import assertk.all
import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.each
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isBetween
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isIn
import assertk.assertions.isInstanceOf
import assertk.assertions.isLessThan
import assertk.assertions.isNotEqualTo
import assertk.assertions.isNotIn
import assertk.assertions.isNotNull
import assertk.assertions.isStrictlyBetween
import assertk.assertions.key
import assertk.assertions.prop
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventTag
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.report.query.QueryAggregationOperator
import io.qalipsis.api.report.query.QueryClause
import io.qalipsis.api.report.query.QueryClauseOperator
import io.qalipsis.api.report.query.QueryDescription
import io.qalipsis.plugins.timescaledb.TestUtils.fibonacciFromSize
import io.qalipsis.plugins.timescaledb.dataprovider.BoundParameters
import io.qalipsis.plugins.timescaledb.dataprovider.DataProviderConfiguration
import io.qalipsis.plugins.timescaledb.dataprovider.PreparedQuery
import io.qalipsis.plugins.timescaledb.utils.DbUtils
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Statement
import jakarta.inject.Inject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

@Testcontainers
@MicronautTest(startApplication = false, environments = ["standalone"])
@Timeout(1, unit = TimeUnit.MINUTES)
internal abstract class AbstractEventQueryGeneratorIntegrationTest : TestPropertyProvider {

    @Inject
    protected lateinit var eventsConverter: TimescaledbEventConverter

    @Inject
    protected lateinit var eventQueryGenerator: AbstractEventQueryGenerator

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

    @Nested
    inner class DataRetrieval {

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
                val events1Tenant1 = fibonacciFromSize(1, 12).map { value ->
                    currentEventTimestamp += timeStep
                    Event(
                        "my-event-1",
                        EventLevel.INFO,
                        timestamp = currentEventTimestamp,
                        value = value,
                        tags = listOf(EventTag("tenant", "tenant-1"), EventTag("value-tag", "$value"))
                    )
                }
                latestTimestamp = currentEventTimestamp
                // Events of name "my-event-2" for the tenant 1.
                currentEventTimestamp = start - timeStep.multipliedBy(2)
                val events2Tenant1 = fibonacciFromSize(5, 12).map { value ->
                    currentEventTimestamp += timeStep
                    Event(
                        "my-event-2",
                        EventLevel.INFO,
                        timestamp = currentEventTimestamp,
                        value = value,
                        tags = listOf(EventTag("tenant", "tenant-1"), EventTag("value-tag", "$value"))
                    )
                }
                // Events of name "my-event-1" for the tenant 2.
                currentEventTimestamp = start - timeStep.multipliedBy(2)
                val events1Tenant2 = fibonacciFromSize(8, 12).map { value ->
                    currentEventTimestamp += timeStep
                    Event(
                        "my-event-1",
                        EventLevel.INFO,
                        timestamp = currentEventTimestamp,
                        value = value,
                        tags = listOf(EventTag("tenant", "tenant-2"), EventTag("value-tag", "$value"))
                    )
                }

                publisher.publish(events1Tenant1 + events2Tenant1 + events1Tenant2)
                delay(1000) // Wait for the transaction to be fully committed.
                initialized = true
            }
        }

        @Test
        internal fun `should fetch the values in the expected tenant`() =
            testDispatcherProvider.run {
                // given
                val queryForTenant1 = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                var result = executeSelect(queryForTenant1, Instant.EPOCH, start + Duration.ofHours(3))

                // then
                assertThat(result).all {
                    hasSize(12)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isEqualTo("my-event-1")
                            key("number").isNotNull()
                        }
                    }
                }

                // when all the events "my-event-2" of tenant-1 are selected.
                val queryForTenant2 = eventQueryGenerator.prepareQueries(
                    "tenant-2", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                result = executeSelect(queryForTenant2, Instant.EPOCH, start + Duration.ofHours(3))

                // then
                assertThat(result).all {
                    hasSize(12)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-2")
                            key("name").isEqualTo("my-event-1")
                            key("number").isNotNull()
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the expected name`() =
            testDispatcherProvider.run {
                // given
                val queryForEvent1 = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                var result = executeSelect(queryForEvent1, Instant.EPOCH, start + Duration.ofHours(3))

                // then
                assertThat(result).all {
                    hasSize(12)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isEqualTo("my-event-1")
                            key("number").isNotNull()
                        }
                    }
                }

                // when all the events "my-event-2" of tenant-1 are selected.
                val queryForEvent2 = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-2")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                result = executeSelect(queryForEvent2, Instant.EPOCH, start + Duration.ofHours(3))

                // then
                assertThat(result).all {
                    hasSize(12)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isEqualTo("my-event-2")
                            key("number").isNotNull()
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the expected time-range`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )

                // when
                var result = executeSelect(query, Instant.EPOCH, start + Duration.ofMinutes(3))

                // then
                assertThat(result).all {
                    hasSize(12)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isNotNull().isInstanceOf(String::class).isEqualTo("my-event-1")
                        }
                    }
                }

                // when
                result = executeSelect(query, start + Duration.ofSeconds(2), start + Duration.ofSeconds(4))

                // then
                assertThat(result).all {
                    hasSize(7)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isNotNull().isInstanceOf(String::class).isEqualTo("my-event-1")
                            key("timestamp").transform { (it as OffsetDateTime).toInstant() }
                                .isBetween(
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
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("value-tag", QueryClauseOperator.IS, "21")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, start, start + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(2)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("tags").isNotNull().isInstanceOf(Json::class).transform { it.asString() }.all {
                                contains("value-tag")
                                contains("21")
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the equal numeric value`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("number", QueryClauseOperator.IS, "21")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, start, start + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(2)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .isEqualTo(21)
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the not equal numeric value`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(
                            QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                            QueryClause("number", QueryClauseOperator.IS_NOT, "21")
                        ),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(11)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isNotNull().isInstanceOf(String::class).isEqualTo("my-event-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .isNotEqualTo(21)
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the greater values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(
                            QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                            QueryClause("number", QueryClauseOperator.IS_GREATER_THAN, "13")
                        ),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(6)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isNotNull().isInstanceOf(String::class).isEqualTo("my-event-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .isGreaterThan(13)
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the greater or equal to values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(
                            QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                            QueryClause("number", QueryClauseOperator.IS_GREATER_OR_EQUAL_TO, "13")
                        ),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(7)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isNotNull().isInstanceOf(String::class).isEqualTo("my-event-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .isGreaterThan(12)
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the lower or equal to values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(
                            QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                            QueryClause("number", QueryClauseOperator.IS_LOWER_OR_EQUAL_TO, "13")
                        ),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(6)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isNotNull().isInstanceOf(String::class).isEqualTo("my-event-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .isLessThan(14)
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the lower than values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(
                            QueryClause("name", QueryClauseOperator.IS, "my-event-1"),
                            QueryClause("number", QueryClauseOperator.IS_LOWER_THAN, "13")
                        ),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(5)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isNotNull().isInstanceOf(String::class).isEqualTo("my-event-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .isLessThan(13)
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the IN numeric values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("number", QueryClauseOperator.IS_IN, "13, 21")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(4)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .isIn(13, 21)
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with NOT IN numeric values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("number", QueryClauseOperator.IS_NOT_IN, "13, 21")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(20)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }.all {
                                isNotEqualTo(13)
                                isNotEqualTo(21)
                            }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the LIKE string single value`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS_LIKE, "mY_ev%")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(24)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isIn("my-event-1", "my-event-2")
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the NOT LIKE string single value`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS_NOT_LIKE, "mY_ev%-2")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(12)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isEqualTo("my-event-1")
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the LIKE string values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS_LIKE, "mY_ev%-1, other")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(12)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isEqualTo("my-event-1")
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the NOT LIKE string values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS_NOT_LIKE, "mY_ev%-1,mY_ev%-2")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).isEmpty()
            }

        @Test
        internal fun `should fetch the values with the IN string values`() =
            testDispatcherProvider.run {
                // given
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS_IN, "my-event-1, my-event-2")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(24)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("name").isIn("my-event-1", "my-event-2")
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with NOT IN number values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("number", QueryClauseOperator.IS_NOT_IN, "13, 21")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(20)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .all {
                                    isNotEqualTo(13)
                                    isNotEqualTo(21)
                                }
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the IN string values for a tag`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("value-tag", QueryClauseOperator.IS_IN, "8,21")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(4)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .isIn(8, 21)
                        }
                    }
                }
            }

        @Test
        internal fun `should fetch the values with the NOT IN string values for a tag`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("value-tag", QueryClauseOperator.IS_NOT_IN, "8,21")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                assertThat(result).all {
                    hasSize(20)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("number").isNotNull().isInstanceOf(BigDecimal::class).transform { it.toInt() }
                                .isNotIn(8, 21)
                        }
                    }
                }
            }

        @Test
        internal fun `should only fetch the expected number of values`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3), limit = 5)

                // then
                assertThat(result).all {
                    hasSize(5)
                    each {
                        it.key("tenant").isEqualTo("tenant-1")
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the descending order by default`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3))

                // then
                var previousTimestamp: Instant = Instant.MAX
                var currentTimestamp: Instant = Instant.MAX
                assertThat(result).all {
                    hasSize(12)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("timestamp").isNotNull().isInstanceOf(OffsetDateTime::class)
                                .transform { it.toInstant().also { currentTimestamp = it } }
                                .isLessThan(previousTimestamp)
                        }
                        previousTimestamp = currentTimestamp
                    }
                }
            }

        @Test
        internal fun `should fetch the values in the ascending order`() =
            testDispatcherProvider.run {
                // given"
                val query = eventQueryGenerator.prepareQueries(
                    "tenant-1", QueryDescription(
                        filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                        timeframeUnit = Duration.ofSeconds(1)
                    )
                )
                val result = executeSelect(query, Instant.EPOCH, latestTimestamp + Duration.ofSeconds(3), order = "ASC")

                // then
                var previousTimestamp: Instant = Instant.EPOCH
                var currentTimestamp: Instant = Instant.EPOCH
                assertThat(result).all {
                    hasSize(12)
                    each {
                        it.all {
                            key("tenant").isEqualTo("tenant-1")
                            key("timestamp").isNotNull().isInstanceOf(OffsetDateTime::class)
                                .transform { it.toInstant().also { currentTimestamp = it } }
                                .isGreaterThan(previousTimestamp)
                        }
                        previousTimestamp = currentTimestamp
                    }
                }
            }
    }

    @Nested
    open inner class AggregationCalculation {

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

                publisher = TimescaledbEventsPublisher(
                    CoroutineScope(SupervisorJob() + Dispatchers.IO),
                    configuration,
                    eventsConverter
                )
                publisher.start()

                var currentEventTimestamp = start - timeStep.multipliedBy(2)
                // Events of name "my-event-1" for the tenant 1.
                val events1Tenant1 = fibonacciFromSize(1, 12).map { value ->
                    currentEventTimestamp += timeStep
                    Event(
                        "my-event-1",
                        EventLevel.INFO,
                        timestamp = currentEventTimestamp,
                        value = value,
                        tags = listOf(EventTag("tenant", "tenant-1"))
                    )
                }
                latestTimestamp = currentEventTimestamp
                // Events of name "my-event-2" for the tenant 1.
                currentEventTimestamp = start - timeStep.multipliedBy(2)
                val events2Tenant1 = fibonacciFromSize(5, 12).map { value ->
                    currentEventTimestamp += timeStep
                    Event(
                        "my-event-2",
                        EventLevel.INFO,
                        timestamp = currentEventTimestamp,
                        value = value,
                        tags = listOf(EventTag("tenant", "tenant-1"))
                    )
                }
                // Events of name "my-event-1" for the tenant 2.
                currentEventTimestamp = start - timeStep.multipliedBy(2)
                val events1Tenant2 = fibonacciFromSize(8, 12).map { value ->
                    currentEventTimestamp += timeStep
                    Event(
                        "my-event-1",
                        EventLevel.INFO,
                        timestamp = currentEventTimestamp,
                        value = value,
                        tags = listOf(EventTag("tenant", "tenant-2"))
                    )
                }

                publisher.publish(events1Tenant1 + events2Tenant1 + events1Tenant2)
                delay(1000) // Wait for the transaction to be fully committed.
                initialized = true
            }
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
                        prop(AggregationPoint::bucket).isEqualTo(start)
                        prop(AggregationPoint::result).isEqualTo(4.5)
                    }
                    index(1).all {
                        prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                        prop(AggregationPoint::result).isEqualTo(30.75)
                    }
                    index(2).all {
                        prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                        prop(AggregationPoint::result).isEqualTo(155.0 + 1.0 / 3)
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
                        prop(AggregationPoint::bucket).isEqualTo(start - Duration.ofSeconds(2))
                        prop(AggregationPoint::result).isEqualTo(1.0)
                    }
                    index(1).all {
                        prop(AggregationPoint::bucket).isEqualTo(start)
                        prop(AggregationPoint::result).isEqualTo(4.5)
                    }
                    index(2).all {
                        prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                        prop(AggregationPoint::result).isEqualTo(30.75)
                    }
                    index(3).all {
                        prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                        prop(AggregationPoint::result).isEqualTo(155.0 + 1.0 / 3)
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
                        prop(AggregationPoint::bucket).isEqualTo(start.truncatedTo(ChronoUnit.SECONDS))
                        prop(AggregationPoint::result).isEqualTo(11.75)
                    }
                    index(1).all {
                        prop(AggregationPoint::bucket).isEqualTo(
                            start.truncatedTo(ChronoUnit.SECONDS) + Duration.ofSeconds(
                                2
                            )
                        )
                        prop(AggregationPoint::result).isEqualTo(80.5)
                    }
                    index(2).all {
                        prop(AggregationPoint::bucket).isEqualTo(
                            start.truncatedTo(ChronoUnit.SECONDS) + Duration.ofSeconds(
                                4
                            )
                        )
                        prop(AggregationPoint::result).isEqualTo(406.0 + (2.0 / 3))
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
                    prop(AggregationPoint::bucket).isEqualTo(start)
                    prop(AggregationPoint::result).isEqualTo(4.0)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isEqualTo(4.0)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isEqualTo(3.0)
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
                    prop(AggregationPoint::bucket).isEqualTo(start)
                    prop(AggregationPoint::result).isEqualTo(4.0)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isEqualTo(4.0)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isEqualTo(3.0)
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
                    prop(AggregationPoint::bucket).isEqualTo(start)
                    prop(AggregationPoint::result).isEqualTo(2.0)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isEqualTo(13.0)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isEqualTo(89.0)
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
                    prop(AggregationPoint::bucket).isEqualTo(start)
                    prop(AggregationPoint::result).isEqualTo(8.0)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isEqualTo(55.0)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isEqualTo(233.0)
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
                    prop(AggregationPoint::bucket).isEqualTo(start)
                    prop(AggregationPoint::result).isEqualTo(18.0)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isEqualTo(123.0)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isEqualTo(466.0)
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
                    prop(AggregationPoint::bucket).isEqualTo(start)
                    prop(AggregationPoint::result).isStrictlyBetween(2.64, 2.65)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isStrictlyBetween(18.33, 18.34)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isStrictlyBetween(72.66, 72.67)
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
                    prop(AggregationPoint::bucket).isEqualTo(start)
                    prop(AggregationPoint::result).isEqualTo(8.0)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isEqualTo(55.0)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isEqualTo(233.0)
                }
            }
        }

        @Test
        internal fun `should calculate the 99 9% percentile values`() = testDispatcherProvider.run {
            // given
            val query = eventQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                    fieldName = "number",
                    aggregationOperation = QueryAggregationOperator.PERCENTILE_99_9,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(AggregationPoint::bucket).isEqualTo(start)
                    prop(AggregationPoint::result).isEqualTo(8.0)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isEqualTo(55.0)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isEqualTo(233.0)
                }
            }
        }
    }

    protected open suspend fun executeSelect(
        query: PreparedQuery,
        start: Instant,
        end: Instant,
        timeframe: Duration? = null,
        limit: Int? = null,
        order: String? = null
    ): List<Map<String, Any?>> {
        val actualTimeframe =
            timeframe?.toMillis() ?: (query.aggregationBoundParameters[":timeframe"]?.value?.toLongOrNull()) ?: 10_000L
        val (actualStart, actualEnd) = roundStartAndEnd(actualTimeframe, start, end)
        val actualLimit = limit ?: query.retrievalBoundParameters[":limit"]?.value
        val actualOrder = order ?: query.retrievalBoundParameters[":order"]?.value as String

        val sqlStatement = String.format(
            query.retrievalStatement.replace("%order%", actualOrder).replace("%limit%", "$actualLimit"), ""
        )

        return Flux.usingWhen(
            connection.create(),
            { connection ->
                Mono.from(connection.createStatement(sqlStatement).also { statement ->
                    bindArguments(statement, query, actualStart, actualEnd)
                }.execute()).flatMapMany { result ->
                    result.map { row, rowMetadata ->
                        rowMetadata.columnNames.associateWith { row[it] }
                            .also {
                                log.trace { "Selected values values at ${it["timestamp"]} : $it}" }
                            }
                    }
                }
            },
            Connection::close
        ).asFlow().toList(mutableListOf<Map<String, Any?>>())
    }

    protected open suspend fun executeAggregation(
        query: PreparedQuery,
        start: Instant,
        end: Instant,
        timeframe: Duration? = null
    ): List<AggregationPoint> {
        val actualTimeframe =
            timeframe?.toMillis() ?: (query.aggregationBoundParameters[":timeframe"]?.value?.toLongOrNull()) ?: 10_000L
        val (actualStart, actualEnd) = roundStartAndEnd(actualTimeframe, start, end)
        val sqlStatement = String.format(query.aggregationStatement.replace("%timeframe%", "$actualTimeframe"), "")

        return Flux.usingWhen(
            connection.create(),
            { connection ->
                Mono.from(connection.createStatement(sqlStatement).also { statement ->
                    bindArguments(statement, query, actualStart, actualEnd)
                }.execute()).flatMapMany { result ->
                    result.map { row, _ ->
                        AggregationPoint(
                            bucket = (row["bucket"] as OffsetDateTime).toInstant(),
                            result = (row["result"] as Number).toDouble()
                        )
                    }
                }
            },
            Connection::close
        ).asFlow().toList(mutableListOf<AggregationPoint>())
    }

    private fun bindArguments(
        statement: Statement,
        query: PreparedQuery,
        actualStart: Instant,
        actualEnd: Instant
    ) {
        // Bind the hard-coded arguments.
        statement.bind(query.aggregationBoundParameters[":start"]!!.identifiers.first(), actualStart)
        statement.bind(query.aggregationBoundParameters[":end"]!!.identifiers.first(), actualEnd)

        // Bind the non-hard-coded arguments.
        query.aggregationBoundParameters.filter { !it.key.startsWith(":") && it.value.identifiers.isNotEmpty() }.values.forEach { (value, type, identifiers) ->
            val actualValue: Any = when (type) {
                BoundParameters.Type.BOOLEAN -> value!!.trim().toBoolean()
                BoundParameters.Type.NUMBER -> value!!.trim().toBigDecimal()
                BoundParameters.Type.NUMBER_ARRAY -> value!!.split(",").map { it.trim().toBigDecimal() }.toTypedArray()
                BoundParameters.Type.STRING -> value!!.trim()
                BoundParameters.Type.STRING_ARRAY -> value!!.split(",").map { it.trim() }.toTypedArray()
            }
            identifiers.forEach { identifier ->
                statement.bind(identifier, actualValue)
            }
        }
    }

    private fun roundStartAndEnd(actualTimeframe: Long, start: Instant, end: Instant): Pair<Instant, Instant> {
        return when {
            actualTimeframe <= Duration.ofSeconds(10)
                .toMillis() -> start.truncatedTo(ChronoUnit.SECONDS) to (end.truncatedTo(ChronoUnit.SECONDS) + Duration.ofSeconds(
                1
            ))
            actualTimeframe <= Duration.ofMinutes(10)
                .toMillis() -> start.truncatedTo(ChronoUnit.MINUTES) to (end.truncatedTo(ChronoUnit.MINUTES) + Duration.ofMinutes(
                1
            ))
            else -> start.truncatedTo(ChronoUnit.HOURS) to (end.truncatedTo(ChronoUnit.HOURS) + Duration.ofHours(1))
        }
    }

    data class AggregationPoint(val bucket: Instant, val result: Double)

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