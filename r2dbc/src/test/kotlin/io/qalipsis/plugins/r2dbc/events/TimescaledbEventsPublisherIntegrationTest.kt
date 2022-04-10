package io.qalipsis.plugins.r2dbc.events

import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventTag
import io.qalipsis.plugins.r2dbc.config.PostgresTestContainerConfiguration.DB_NAME
import io.qalipsis.plugins.r2dbc.config.PostgresTestContainerConfiguration.PASSWORD
import io.qalipsis.plugins.r2dbc.config.PostgresTestContainerConfiguration.USERNAME
import io.qalipsis.plugins.r2dbc.config.PostgresqlTemplateTest
import io.qalipsis.plugins.r2dbc.config.TimescaledbEventsConfiguration
import io.qalipsis.test.mockk.relaxedMockk
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 *
 * @author Gabriel Moraes
 */
@Testcontainers
@Timeout(1, unit = TimeUnit.MINUTES)
internal class TimescaledbEventsPublisherIntegrationTest : PostgresqlTemplateTest() {

    // The meter registry should provide a timer that execute the expressions to record.
    private val meterRegistry: MeterRegistry = relaxedMockk {
        every { timer(any(), *anyVararg()) } returns relaxedMockk {
            every { record(any<Runnable>()) } answers { (firstArg() as Runnable).run() }
        }
    }

    private lateinit var configuration: TimescaledbEventsConfiguration

    private val eventsConverter = TimescaledbEventConverter()

    private lateinit var connectionPool: ConnectionPool

    @BeforeAll
    internal fun setUp() {
        configuration = mockk {
            every { host } returns getHost()
            every { minLevel } returns EventLevel.TRACE
            every { username } returns USERNAME
            every { password } returns PASSWORD
            every { lingerPeriod } returns Duration.ofNanos(1)
            every { batchSize } returns 2
            every { publishers } returns 1
        }

        connectionPool = ConnectionPool(
            ConnectionPoolConfiguration.builder()
                .connectionFactory(
                    PostgresqlConnectionFactory(
                        PostgresqlConnectionConfiguration.builder().host("localhost")
                            .password(PASSWORD).username(USERNAME)
                            .database(DB_NAME)
                            .schema("qalipsis")
                            .port(pgsqlContainer.getMappedPort(5432))
                            .build()
                    )
                ).build()
        )
    }

    @Test
    @Timeout(30)
     fun `should save events data`() = testDispatcherProvider.run {
        // given
        val publisher = TimescaledbEventsPublisher(
            this,
            this.coroutineContext,
            configuration,
            meterRegistry,
            eventsConverter,
            connectionPool
        )
        publisher.start()

        // when
        publisher.coInvokeInvisible<Void>("performPublish", getEvents())

        // then
        val result = connectionPool.create().flatMap {
            val statement = it.createStatement("select count(*) from events")
            Mono.from(statement.execute())
                .map { it.map { row, _ -> row.get(0) as Long } }
                .doOnTerminate { Mono.from(it.close()).subscribe() }
        }.awaitFirstOrNull()?.awaitFirstOrNull()

        assertEquals(1, result)

        publisher.stop()
    }

    private fun getEvents(): List<Event> {
        val events = mutableListOf<Event>()
        events.add(
            Event(
                name = "my-number",
                EventLevel.INFO,
                tags = listOf(EventTag("key-1", "value-1"), EventTag("key-2", "value-2")),
                value = 123.2
            )
        )
        return events
    }

}