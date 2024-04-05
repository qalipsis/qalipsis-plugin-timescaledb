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
import io.aerisconsulting.catadioptre.setProperty
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Timer
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.report.TimeSeriesMeter
import io.qalipsis.api.report.TimeSeriesRecord
import io.qalipsis.plugins.timescaledb.TimescaleDbContainerProvider
import io.qalipsis.plugins.timescaledb.meter.TimescaledbMeterRegistry
import io.qalipsis.plugins.timescaledb.utils.DbUtils
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.Connection
import jakarta.annotation.PostConstruct
import jakarta.inject.Inject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.pow


@WithMockk
@Testcontainers
@MicronautTest(environments = ["timescaledb"], startApplication = false)
internal class TimeSeriesMeterRecordConverterIntegrationTest : TestPropertyProvider {

    @Inject
    private lateinit var meterRegistry: TimescaledbMeterRegistry

    @Inject
    private lateinit var timeSeriesMeterRecordConverter: TimeSeriesMeterRecordConverter

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var connection: ConnectionPool

    @MockK("The mocked clock")
    private lateinit var clock: Clock

    private lateinit var meter: Meter

    @PostConstruct
    fun init() {
        meterRegistry.setProperty("clock", clock)
    }

    @AfterEach
    internal fun tearDown() {
        meterRegistry.remove(meter)
        clearDatabase()
    }

    private fun clearDatabase() {
        Flux.usingWhen(
            connection.create(),
            { connection -> Mono.from(connection.createStatement("TRUNCATE TABLE meters").execute()) },
            Connection::close
        ).blockLast()
    }

    @Test
    internal fun `should deserialize a minimal meter`() {
        // given
        val clockTime = mockClock()
        meter = meterRegistry.timer("my-meter")

        // when
        meterRegistry.publish()

        // then
        val result = readRecords()
        assertThat(result.last()).isDataClassEqualTo(
            TimeSeriesMeter(
                name = "my-meter",
                timestamp = clockTime,
                type = "timer",
                count = 0L,
                meanDuration = Duration.ZERO,
                maxDuration = Duration.ZERO,
                sumDuration = Duration.ZERO
            )
        )
    }

    @Test
    internal fun `should deserialize a timer`() {
        // given
        val initClockTime = mockClock()
        var sum = 0
        meter = Timer.builder("my-meter")
            .tags(
                "tenant", "my-tenant", "campaign", "my-campaign", "scenario", "my-scenario",
                "tag-1", "value-1", "tag-2", "value-2"
            )
            .register(meterRegistry)
            .apply {
                repeat(100) {
                    sum += (it + 1)
                    record(Duration.ofMinutes(it + 1L))
                }
            }

        // when
        val publishClockTime = mockClock(initClockTime + METER_REGISTRY_STEP)
        meterRegistry.publish()

        // then
        val result = readRecords("WHERE tenant = 'my-tenant'")
        assertThat(result).all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesMeter(
                    name = "my-meter",
                    timestamp = publishClockTime,
                    campaign = "my-campaign",
                    scenario = "my-scenario",
                    tags = mapOf("tag-1" to "value-1", "tag-2" to "value-2"),
                    type = "timer",
                    count = 100L,
                    meanDuration = Duration.ofMinutes(50).plusSeconds(30),
                    maxDuration = Duration.ofMinutes(100),
                    sumDuration = Duration.ofMinutes(sum.toLong()),
                )
            )
        }
    }

    @Test
    internal fun `should deserialize a gauge`() {
        // given
        val initClockTime = mockClock()
        meter = Gauge.builder("my-meter") { AtomicInteger(564) }
            .tags(
                "tenant", "my-tenant", "campaign", "my-campaign", "scenario", "my-scenario",
                "tag-1", "value-1", "tag-2", "value-2"
            )
            .register(meterRegistry)

        // when
        val publishClockTime = mockClock(initClockTime + METER_REGISTRY_STEP)
        meterRegistry.publish()

        // then
        val result = readRecords("WHERE tenant = 'my-tenant'")
        assertThat(result).all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesMeter(
                    name = "my-meter",
                    timestamp = publishClockTime,
                    campaign = "my-campaign",
                    scenario = "my-scenario",
                    tags = mapOf("tag-1" to "value-1", "tag-2" to "value-2"),
                    type = "gauge",
                    value = BigDecimal("564.000000")
                )
            )
        }
    }

    @Test
    internal fun `should deserialize a counter`() {
        // given
        val initClockTime = mockClock()
        var sum = 0
        meter = Counter.builder("my-meter")
            .tags(
                "tenant", "my-tenant", "campaign", "my-campaign", "scenario", "my-scenario",
                "tag-1", "value-1", "tag-2", "value-2"
            )
            .register(meterRegistry)
            .apply {
                repeat(100) {
                    sum += (it + 1)
                    this.increment(it + 1.0)
                }
            }

        // when
        val publishClockTime = mockClock(initClockTime + METER_REGISTRY_STEP)
        meterRegistry.publish()

        // then
        val result = readRecords("WHERE tenant = 'my-tenant'")
        assertThat(result).all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesMeter(
                    name = "my-meter",
                    timestamp = publishClockTime,
                    campaign = "my-campaign",
                    scenario = "my-scenario",
                    tags = mapOf("tag-1" to "value-1", "tag-2" to "value-2"),
                    type = "counter",
                    count = 5050L
                )
            )
        }
    }

    @Test
    internal fun `should deserialize a distribution summary`() {
        // given
        val initClockTime = mockClock()
        var sum = 0
        meter = DistributionSummary.builder("my-meter")
            .tags(
                "tenant", "my-tenant", "campaign", "my-campaign", "scenario", "my-scenario",
                "tag-1", "value-1", "tag-2", "value-2"
            )
            .register(meterRegistry)
            .apply {
                repeat(100) {
                    sum += (it + 1)
                    this.record(it + 1.0)
                }
            }

        // when
        val publishClockTime = mockClock(initClockTime + METER_REGISTRY_STEP)
        meterRegistry.publish()

        // then
        val result = readRecords("WHERE tenant = 'my-tenant'")
        assertThat(result).all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesMeter(
                    name = "my-meter",
                    timestamp = publishClockTime,
                    campaign = "my-campaign",
                    scenario = "my-scenario",
                    tags = mapOf("tag-1" to "value-1", "tag-2" to "value-2"),
                    type = "distribution_summary",
                    count = 100L,
                    mean = BigDecimal("50.500000"),
                    max = BigDecimal("100.000000"),
                    sum = BigDecimal("$sum.000000"),
                )
            )
        }
    }

    private fun mockClock(instant: Instant = Instant.now().truncatedTo(ChronoUnit.HOURS)): Instant {
        every { clock.wallTime() } returns instant.toEpochMilli()
        return instant
    }

    private fun readRecords(clause: String = ""): List<TimeSeriesRecord> {
        return Flux.usingWhen(
            connection.create(),
            { connection ->
                Mono.from(connection.createStatement("SELECT * FROM meters $clause").execute())
                    .flatMapMany { result ->
                        result.map { row, rowMetadata ->
                            timeSeriesMeterRecordConverter.convert(row, rowMetadata)
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
            override val initSchema: Boolean = false
        })

        return mapOf(
            "meters.export.enabled" to "true",
            "meters.export.timescaledb.enabled" to "true",
            "meters.export.timescaledb.host" to "localhost",
            "meters.export.timescaledb.port" to "${db.firstMappedPort}",
            "meters.export.timescaledb.database" to DB_NAME,
            "meters.export.timescaledb.username" to USERNAME,
            "meters.export.timescaledb.password" to PASSWORD,
            "meters.export.timescaledb.schema" to SCHEMA,
            "meters.export.timescaledb.step" to "$METER_REGISTRY_STEP",
            "meters.export.timescaledb.autostart" to "false",
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
        const val SCHEMA = "meters"

        val METER_REGISTRY_STEP = Duration.ofSeconds(10)

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