package io.qalipsis.plugins.timescaledb.event

import assertk.all
import assertk.assertThat
import assertk.assertions.containsOnly
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.key
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventTag
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.timescaledb.event.catadioptre.doPerformPublish
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.Connection
import jakarta.inject.Inject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.TimeUnit

@Testcontainers
@MicronautTest(environments = ["timescaledb", "head"], startApplication = false)
@Timeout(1, unit = TimeUnit.MINUTES)
internal abstract class AbstractEventDataProviderIntegrationTest : TestPropertyProvider {

    @Inject
    private lateinit var eventDataProvider: TimescaledbEventDataProvider

    @Inject
    private lateinit var eventsConverter: TimescaledbEventConverter

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var connection: PostgresqlConnectionFactory

    private lateinit var publisher: TimescaledbEventsPublisher

    abstract val dbPort: Int

    @BeforeEach
    fun setUpAll() {
        connection = PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host("localhost").port(dbPort)
                .username(USERNAME)
                .password(PASSWORD)
                .database(DB_NAME)
                .schema(SCHEMA)
                .build()
        )
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
            TimescaledbEventsPublisher(CoroutineScope(SupervisorJob() + Dispatchers.IO), configuration, eventsConverter)
        publisher.start()
    }

    @AfterEach
    fun tearDown() {
        Flux.usingWhen(
            connection.create(),
            { connection -> Mono.from(connection.createStatement("truncate table events").execute()) },
            Connection::close
        ).blockLast()
    }

    override fun getProperties(): Map<String, String> = mapOf(
        "events.provider.timescaledb.enabled" to "true",
        "events.provider.timescaledb.host" to "localhost",
        "events.provider.timescaledb.port" to "$dbPort",
        "events.provider.timescaledb.database" to DB_NAME,
        "events.provider.timescaledb.username" to USERNAME,
        "events.provider.timescaledb.password" to PASSWORD,
        "events.provider.timescaledb.schema" to SCHEMA,
    )

    @Test
    @Timeout(10)
    internal fun `should list the names without filter`() = testDispatcherProvider.run {
        // given
        // Index of constant size are used to make the verification of the alpha sorting easier.
        publisher.doPerformPublish((100..199).flatMap {
            listOf(
                // In the tenant 1, events are saved twice to verify the distinct.
                Event("my-event-$it-info", EventLevel.INFO, tags = listOf(EventTag("tenant", "tenant-1"))),
                Event("my-event-$it-info", EventLevel.INFO, tags = listOf(EventTag("tenant", "tenant-1"))),
                Event("my-event-$it-warn", EventLevel.WARN, tags = listOf(EventTag("tenant", "tenant-2")))
            )
        })

        // when
        val allNamesOfTenant1 = eventDataProvider.searchNames("tenant-1", emptySet(), 200)

        // then
        assertThat(allNamesOfTenant1.toList()).all {
            hasSize(100)
            (0..99).forEach { index ->
                index(index).isEqualTo("my-event-${100 + index}-info")
            }
        }

        // when
        val someNamesOfTenant2 = eventDataProvider.searchNames("tenant-2", emptySet(), 30)

        // then
        assertThat(someNamesOfTenant2.toList()).all {
            hasSize(30)
            (0..29).forEach { index ->
                index(index).isEqualTo("my-event-${100 + index}-warn")
            }
        }
    }

    @Test
    @Timeout(10)
    internal fun `should list the names with filter`() = testDispatcherProvider.run {
        // given
        // Index of constant size are used to make the verification of the alpha sorting easier.
        publisher.doPerformPublish((100..199).flatMap {
            listOf(
                Event("my-event-$it-info", EventLevel.INFO, tags = listOf(EventTag("tenant", "tenant-1"))),
                Event("my-event-$it-warn", EventLevel.WARN, tags = listOf(EventTag("tenant", "tenant-2")))
            )
        })
        val filters = setOf("mY-eVenT-10*", "*-1?9-*")

        // when
        var result = eventDataProvider.searchNames("tenant-1", filters, 20)

        // then
        assertThat(result).all {
            hasSize(19)
            containsOnly(
                "my-event-100-info",
                "my-event-101-info",
                "my-event-102-info",
                "my-event-103-info",
                "my-event-104-info",
                "my-event-105-info",
                "my-event-106-info",
                "my-event-107-info",
                "my-event-108-info",
                "my-event-109-info",
                "my-event-119-info",
                "my-event-129-info",
                "my-event-139-info",
                "my-event-149-info",
                "my-event-159-info",
                "my-event-169-info",
                "my-event-179-info",
                "my-event-189-info",
                "my-event-199-info"
            )
        }

        // when
        result = eventDataProvider.searchNames("tenant-2", filters, 5)

        // then
        assertThat(result).all {
            hasSize(5)
            containsOnly(
                "my-event-100-warn",
                "my-event-101-warn",
                "my-event-102-warn",
                "my-event-103-warn",
                "my-event-104-warn"
            )
        }
    }


    @Test
    @Timeout(10)
    internal fun `should list the tags without filters`() = testDispatcherProvider.run {
        // given
        publisher.doPerformPublish(
            listOf(
                Event(
                    RandomStringUtils.randomAlphabetic(5),
                    EventLevel.INFO,
                    tags = listOf(EventTag("tenant", "tenant-1"), EventTag("tag-1", "value-1"))
                ),
                Event(
                    RandomStringUtils.randomAlphabetic(5),
                    EventLevel.INFO,
                    tags = listOf(
                        EventTag("tenant", "tenant-1"),
                        EventTag("campaign", "campaign-1"),
                        EventTag("scenario", "scenario-1"),
                        EventTag("tag-1", "value-2"),
                        EventTag("tag-2", "value-2"),
                        EventTag("tag-3", "value-3")
                    )
                ),
                Event(
                    RandomStringUtils.randomAlphabetic(5),
                    EventLevel.WARN,
                    tags = listOf(EventTag("tenant", "tenant-2"), EventTag("tag-2", "value-3"))
                )
            )
        )

        // when
        val allTagsOfTenant1 = eventDataProvider.searchTagsAndValues("tenant-1", emptySet(), 200)

        // then
        assertThat(allTagsOfTenant1).all {
            hasSize(3)
            key("tag-1").containsOnly("value-1", "value-2")
            key("tag-2").containsOnly("value-2")
            key("tag-3").containsOnly("value-3")
        }

        // when
        val allTagsOfTenant2 = eventDataProvider.searchTagsAndValues("tenant-2", emptySet(), 200)

        // then
        assertThat(allTagsOfTenant2).all {
            hasSize(1)
            key("tag-2").containsOnly("value-3")
        }

        // when
        val someTagsOfTenant1 = eventDataProvider.searchTagsAndValues("tenant-1", emptySet(), 2)

        // then
        assertThat(someTagsOfTenant1).all {
            hasSize(2)
            key("tag-1").containsOnly("value-1", "value-2")
            key("tag-2").containsOnly("value-2")
        }
    }

    @Test
    @Timeout(10)
    internal fun `should list the tags with filter`() = testDispatcherProvider.run {
        // given
        publisher.doPerformPublish(
            listOf(
                Event(
                    RandomStringUtils.randomAlphabetic(5),
                    EventLevel.INFO,
                    tags = listOf(EventTag("tenant", "tenant-1"), EventTag("tag-1", "value-1"))
                ),
                Event(
                    RandomStringUtils.randomAlphabetic(5),
                    EventLevel.INFO,
                    tags = listOf(
                        EventTag("tenant", "tenant-1"),
                        EventTag("campaign", "campaign-1"),
                        EventTag("scenario", "scenario-1"),
                        EventTag("tag-1", "value-2"),
                        EventTag("tag-2", "value-2"),
                        EventTag("tag-3", "value-3")
                    )
                ),
                Event(
                    RandomStringUtils.randomAlphabetic(5),
                    EventLevel.WARN,
                    tags = listOf(EventTag("tenant", "tenant-2"), EventTag("tag-2", "value-3"))
                )
            )
        )

        // when
        var result = eventDataProvider.searchTagsAndValues("tenant-1", setOf("tag-?"), 200)

        // then
        assertThat(result).all {
            hasSize(3)
            key("tag-1").containsOnly("value-1", "value-2")
            key("tag-2").containsOnly("value-2")
            key("tag-3").containsOnly("value-3")
        }

        // when
        result = eventDataProvider.searchTagsAndValues("tenant-1", setOf("*-2"), 200)

        // then
        assertThat(result).all {
            hasSize(2)
            key("tag-1").containsOnly("value-2")
            key("tag-2").containsOnly("value-2")
        }

        // when
        result = eventDataProvider.searchTagsAndValues("tenant-1", setOf("*g-2"), 200)

        // then
        assertThat(result).all {
            hasSize(1)
            key("tag-2").containsOnly("value-2")
        }

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