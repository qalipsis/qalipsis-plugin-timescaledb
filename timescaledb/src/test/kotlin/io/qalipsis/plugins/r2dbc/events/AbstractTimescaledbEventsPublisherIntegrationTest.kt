package io.qalipsis.plugins.r2dbc.events

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isTrue
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventGeoPoint
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventRange
import io.qalipsis.api.events.EventTag
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.r2dbc.config.TimescaledbEventsConfiguration
import io.qalipsis.plugins.r2dbc.events.catadioptre.doPerformPublish
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.postgresql.codec.Json
import jakarta.inject.Inject
import kotlinx.coroutines.delay
import kotlinx.coroutines.reactive.awaitLast
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Mono
import java.io.PrintWriter
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import kotlin.reflect.jvm.jvmName
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

@OptIn(ExperimentalTime::class)
@Testcontainers
@MicronautTest(environments = ["timescaledb"], startApplication = false)
@Timeout(1, unit = TimeUnit.MINUTES)
internal abstract class AbstractTimescaledbEventsPublisherIntegrationTest : TestPropertyProvider {

    @Inject
    private lateinit var eventsConverter: TimescaledbEventConverter

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var connection: PostgresqlConnectionFactory

    abstract val dbPort: Int

    @BeforeEach
    fun setUpAll() {
        connection = PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host("localhost").port(dbPort)
                .username(USERNAME).password(PASSWORD)
                .database(DB_NAME)
                .schema("events")
                .build()
        )
    }

    @AfterEach
    fun tearDown() {
        connection.create()
            .flatMap { connection ->
                Mono.from(connection.createStatement("truncate table events").execute())
            }.block()
    }

    @Test
    @Timeout(10)
    fun `should save events data`() = testDispatcherProvider.run {
        // given
        val configuration = mockk<TimescaledbEventsConfiguration> {
            every { host } returns "localhost"
            every { port } returns dbPort
            every { database } returns DB_NAME
            every { schema } returns "events"
            every { username } returns USERNAME
            every { password } returns PASSWORD
            every { minLevel } returns EventLevel.TRACE
            every { lingerPeriod } returns Duration.ofNanos(1)
            every { batchSize } returns 2000
            every { publishers } returns 1
        }
        val publisher = TimescaledbEventsPublisher(this, configuration, eventsConverter)
        publisher.start()
        val events = mutableListOf<Event>()
        events.add(Event(name = "my-event-A", EventLevel.INFO))
        events.add(
            Event(
                name = "my-event-B",
                EventLevel.INFO,
                tags = listOf(EventTag("key-1", "value-1"), EventTag("key-2", "value-2"))
            )
        )
        val instantNow = Instant.now().minusSeconds(12).truncatedTo(ChronoUnit.MILLIS)
        val zdtNow = ZonedDateTime.now(Clock.systemUTC().zone).truncatedTo(ChronoUnit.MILLIS)
        val ldtNow = LocalDateTime.now().plusDays(1).truncatedTo(ChronoUnit.MILLIS)
        val values = createTestData(instantNow, zdtNow, ldtNow)

        events += values.keys.mapIndexed { index, value ->
            Event(name = "my-event-$index", EventLevel.INFO, value = value)
        }

        // when
        publisher.doPerformPublish(events)

        // then
        publisher.stop()
        val rows = executeSelect("select * from events")

        assertThat(rows).hasSize(events.size)
        values.forEach { (value, assertion) ->
            assertThat(rows).transform("Finding $value as ${value::class.jvmName}") { it.any(assertion) }.isTrue()
        }
    }

    /**
     * Creates the test data set with the value to log as key and the condition
     * to assertion to match when verifying the retrieved values.
     */
    private fun createTestData(
        instantNow: Instant,
        zdtNow: ZonedDateTime,
        ldtNow: LocalDateTime
    ): Map<Any, ((Map<String, *>) -> Boolean)> {
        return linkedMapOf(
            "my-message" to { it["message"] == "my-message" },
            true to { it["boolean"] == true },
            123.65 to { (it["number"] as BigDecimal?)?.toDouble() == 123.65 },
            123.65F to { (it["number"] as BigDecimal?)?.toDouble() == 123.65 },
            123.65.toBigDecimal() to { (it["number"] as BigDecimal?)?.toDouble() == 123.65 },
            123 to { (it["number"] as BigDecimal?)?.toDouble() == 123.0 },
            123.toBigInteger() to { (it["number"] as BigDecimal?)?.toDouble() == 123.0 },
            123L to { (it["number"] as BigDecimal?)?.toDouble() == 123.0 },
            instantNow to { (it["date"] as OffsetDateTime?)?.toInstant() == instantNow },
            zdtNow to {
                (it["date"] as OffsetDateTime?)?.toInstant() == zdtNow.toInstant()
            },
            ldtNow to {
                (it["date"] as OffsetDateTime?)?.toInstant() == ldtNow.atZone(ZoneId.systemDefault()).toInstant()
            },
            relaxedMockk<Throwable> {
                every<String?> { message } returns "my-error"
                every<Unit> { printStackTrace(any<PrintWriter>()) } answers {
                    (firstArg() as PrintWriter).write("this is the stack")
                }
            } to { it["error"] == "my-error" && it["stack_trace"] == "this is the stack" },
            Duration.ofNanos(12_123_456_789) to { it["duration"] == 12_123_456_789L },
            EventGeoPoint(
                12.34,
                34.76
            ) to { (it["geo_point"] as Json?)?.asString() == """{"type": "Point", "coordinates": [12.34, 34.76]}""" },
            EventRange(12.34, 34.76, includeUpper = false) to {
                (it["value"] as Json?)?.asString() == """{"lowerBound": 12.34, "upperBound": 34.76, "includeLower": true, "includeUpper": false}"""
            },
            MyTestObject() to {
                (it["value"] as Json?)?.asString() == """{"property1": 1243.65, "property2": "here is the test"}"""
            }
        )
    }

    @Test
    @Timeout(30)
    fun `should save a massive amount of events`() = testDispatcherProvider.run {
        // given
        val totalCount = 100_000
        val configuration = mockk<TimescaledbEventsConfiguration> {
            every { host } returns "localhost"
            every { port } returns dbPort
            every { database } returns DB_NAME
            every { schema } returns "events"
            every { username } returns USERNAME
            every { password } returns PASSWORD
            every { minLevel } returns EventLevel.TRACE
            every { lingerPeriod } returns Duration.ofSeconds(2)
            every { batchSize } returns 20_000
            every { publishers } returns 5
        }
        val publisher = TimescaledbEventsPublisher(this, configuration, eventsConverter)
        publisher.start()

        // when
        repeat(totalCount) { index ->
            publisher.publish(
                Event(
                    name = "my-event",
                    EventLevel.INFO,
                    tags = listOf(EventTag("key-1", "value-1"), EventTag("key-2", "value-2")),
                    value = "Message #$index"
                )
            )
        }

        // then
        val elapsed = measureTime {
            do {
                delay(1000)
                val savedEventsCount = (executeSelect("select count(*) as c from events").first()["c"] as Long).toInt()
                log.info { "Saved events so far: $savedEventsCount" }
            } while (savedEventsCount != totalCount)
        }.inWholeSeconds
        println("$totalCount events were saved in $elapsed sec (avg: ${totalCount / elapsed} records / sec)")
    }

    private suspend fun executeSelect(statement: String): List<Map<String, *>> {
        return connection.create()
            .flatMap { connection ->
                Mono.from(connection.createStatement(statement).execute())
            }.flatMapMany { result ->
                result.map { row, rowMetadata -> rowMetadata.columnNames.associateWith { name -> row[name] } }
            }.collectList().awaitLast()
    }

    data class MyTestObject(val property1: Double = 1243.65, val property2: String = "here is the test")

    override fun getProperties(): Map<String, String> = mapOf(
        "pgsql.host" to "localhost",
        "pgsql.port" to "$dbPort",
        "pgsql.database" to DB_NAME,
        "pgsql.username" to USERNAME,
        "pgsql.username" to PASSWORD
    )

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

    }
}