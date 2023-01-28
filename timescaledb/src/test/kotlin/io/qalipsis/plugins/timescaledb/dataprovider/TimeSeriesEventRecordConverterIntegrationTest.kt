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

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isDataClassEqualTo
import assertk.assertions.isNotNull
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventTag
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.report.TimeSeriesEvent
import io.qalipsis.api.report.TimeSeriesRecord
import io.qalipsis.plugins.timescaledb.TimescaleDbContainerProvider
import io.qalipsis.plugins.timescaledb.event.TimescaledbEventConverter
import io.qalipsis.plugins.timescaledb.event.TimescaledbEventsPublisher
import io.qalipsis.plugins.timescaledb.event.catadioptre.doPerformPublish
import io.qalipsis.plugins.timescaledb.event.catadioptre.stackTraceToString
import io.qalipsis.plugins.timescaledb.utils.DbUtils
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.Connection
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.inject.Inject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import kotlin.math.pow


@Testcontainers
@MicronautTest(environments = ["timescaledb"], startApplication = false)
@Timeout(1, unit = TimeUnit.MINUTES)
internal class TimeSeriesEventRecordConverterIntegrationTest : TestPropertyProvider {

    @Inject
    private lateinit var publisher: TimescaledbEventsPublisher

    @Inject
    private lateinit var timeSeriesEventRecordConverter: TimeSeriesEventRecordConverter

    @Inject
    private lateinit var timescaledbEventConverter: TimescaledbEventConverter

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var connection: ConnectionPool

    @PostConstruct
    fun init() {
        publisher.start()
    }

    @PreDestroy
    fun close() {
        publisher.stop()
    }

    @AfterEach
    internal fun tearDown() {
        Flux.usingWhen(
            connection.create(),
            { connection -> Mono.from(connection.createStatement("TRUNCATE TABLE events").execute()) },
            Connection::close
        ).blockLast()
    }

    @Test
    internal fun `should deserialize a minimal event`() {
        // given
        val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val event = Event(
            name = "my-event-1",
            timestamp = timestamp,
            level = EventLevel.INFO
        )

        // when
        publisher.doPerformPublish(listOf(event))

        // then
        val result = readRecords()
        assertThat(result).isNotNull().all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesEvent(
                    name = "my-event-1",
                    timestamp = timestamp,
                    level = "info"
                )
            )
        }
    }

    @Test
    internal fun `should deserialize a complete event`() {
        // given
        val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val date = timestamp.minusMillis(12_654_123)
        val exception = RuntimeException("There is an error")
        val event = Event(
            name = "my-event-2",
            timestamp = timestamp,
            level = EventLevel.DEBUG,
            tags = listOf(
                EventTag("tenant", "my-tenant"),
                EventTag("campaign", "my-campaign"),
                EventTag("scenario", "my-scenario"),
                EventTag("tag-1", "value-1"),
                EventTag("tag-2", "value-2")
            ),
            value = listOf<Any>(
                date,
                exception,
                "This is a message",
                true,
                1254.267,
                Duration.ofSeconds(123)
            )
        )

        // when
        publisher.doPerformPublish(listOf(event))

        // then
        val result = readRecords()
        assertThat(result).isNotNull().all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesEvent(
                    name = "my-event-2",
                    timestamp = timestamp,
                    level = "debug",
                    tags = mapOf("tag-1" to "value-1", "tag-2" to "value-2"),
                    campaign = "my-campaign",
                    scenario = "my-scenario",
                    message = "This is a message",
                    error = "There is an error",
                    stackTrace = timescaledbEventConverter.stackTraceToString(exception) as String,
                    date = date,
                    boolean = true,
                    number = BigDecimal("1254.267000"),
                    duration = Duration.ofSeconds(123)
                )
            )
        }
    }

    private fun readRecords(): List<TimeSeriesRecord> {
        return Flux.usingWhen(
            connection.create(),
            { connection ->
                Mono.from(connection.createStatement("SELECT * FROM events").execute())
                    .flatMapMany { result ->
                        result.map { row, rowMetadata ->
                            timeSeriesEventRecordConverter.convert(row, rowMetadata)
                        }
                    }
            },
            Connection::close
        ).collectList().block()!!
    }

    override fun getProperties(): Map<String, String> {
        connection = DbUtils.createConnectionPool(object : DataProviderConfiguration {
            override val host: String = "localhost"
            override val port: Int = db.firstMappedPort
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

        return mapOf(
            "events.export.timescaledb.enabled" to StringUtils.TRUE,
            "events.export.timescaledb.host" to "localhost",
            "events.export.timescaledb.port" to "${db.firstMappedPort}",
            "events.export.timescaledb.database" to DB_NAME,
            "events.export.timescaledb.username" to USERNAME,
            "events.export.timescaledb.password" to PASSWORD,
            "events.export.timescaledb.schema" to SCHEMA,
            "events.export.timescaledb.min-level" to "TRACE",
            "events.export.timescaledb.publishers" to "1",
            "events.export.timescaledb.batchSize" to "50000",
            "events.export.timescaledb.linger-period" to "60s"
        )
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

        @Container
        @JvmStatic
        val db: JdbcDatabaseContainer<*> = TimescaleDbContainerProvider().newInstance().apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(128 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            waitingFor(Wait.forListeningPort())
            withStartupTimeout(Duration.ofSeconds(240))

            withDatabaseName(DB_NAME)
            withUsername(USERNAME)
            withPassword(PASSWORD)
            withCommand("postgres -c shared_preload_libraries=timescaledb -c log_error_verbosity=VERBOSE -c timescaledb.telemetry_level=OFF -c max_connections=100")
            withInitScript("pgsql-init.sql")
        }
    }
}