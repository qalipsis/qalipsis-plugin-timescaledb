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
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.Counter
import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.DistributionSummary
import io.qalipsis.api.meters.Gauge
import io.qalipsis.api.meters.MeasurementMetric
import io.qalipsis.api.meters.Meter
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.meters.MeterType
import io.qalipsis.api.meters.Statistic
import io.qalipsis.api.meters.Timer
import io.qalipsis.api.report.TimeSeriesMeter
import io.qalipsis.api.report.TimeSeriesRecord
import io.qalipsis.plugins.timescaledb.TimescaleDbContainerProvider
import io.qalipsis.plugins.timescaledb.meter.TimescaledbMeasurementPublisher
import io.qalipsis.plugins.timescaledb.meter.TimescaledbMeasurementPublisherFactory
import io.qalipsis.plugins.timescaledb.utils.DbUtils
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.Connection
import jakarta.inject.Inject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
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
import kotlin.math.pow


@WithMockk
@Testcontainers
@MicronautTest(environments = ["timescaledb"], startApplication = false)
internal class TimeSeriesMeterRecordConverterIntegrationTest : TestPropertyProvider {

    @Inject
    protected lateinit var measurementPublisherFactory: TimescaledbMeasurementPublisherFactory

    private lateinit var timescaledbMeasurementPublisher: TimescaledbMeasurementPublisher

    @Inject
    private lateinit var timeSeriesMeterRecordConverter: TimeSeriesMeterRecordConverter

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var connection: ConnectionPool

    @BeforeEach
    fun initPublisher() {
        timescaledbMeasurementPublisher = measurementPublisherFactory.getPublisher() as TimescaledbMeasurementPublisher
    }

    @AfterEach
    internal fun tearDown() {
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
    internal fun `should deserialize a minimal meter`() = testDispatcherProvider.run {
        // given
        val now = Instant.now()
        val timer = mockk<Timer> {
            every { id } returns mockk<Meter.Id> {
                every { meterName } returns "my-meter"
                every { tags } returns emptyMap()
                every { type } returns MeterType.TIMER
                every { scenarioName } returns "SCENARIO one"
                every { campaignKey } returns "first campaign 5473653"
                every { stepName } returns "step uno"
            }
        }

        // when
        timescaledbMeasurementPublisher.publish(listOf(mockk<MeterSnapshot<Timer>> {
            every { timestamp } returns now
            every { meter } returns timer
            every { measurements } returns listOf(MeasurementMetric(9.0, Statistic.VALUE))
        }))

        // then
        val result = readRecords()
        assertThat(result.last()).isDataClassEqualTo(
            TimeSeriesMeter(
                name = "my-meter",
                timestamp = now,
                type = "timer",
                count = 9L,
                meanDuration = Duration.ZERO,
                maxDuration = Duration.ZERO,
                sumDuration = Duration.ZERO,
                campaign = "first campaign 5473653",
                scenario = "scenario one"
            )
        )
    }

    @Test
    internal fun `should deserialize a timer`() = testDispatcherProvider.run {
        // given
        val now = Instant.now()
        val timer = mockk<Timer> {
            every { id } returns mockk<Meter.Id> {
                every { meterName } returns "my-meter"
                every { tags } returns mapOf(
                    "tenant" to "my-tenant", "campaign" to "my-campaign", "scenario" to "my-scenario",
                    "tag-1" to "value-1", "tag-2" to "value-2"
                )
                every { type } returns MeterType.TIMER
                every { scenarioName } returns "my-scenario"
                every { campaignKey } returns "my-campaign"
                every { stepName } returns "step uno"
            }
        }

        // when
        timescaledbMeasurementPublisher.publish(listOf(mockk<MeterSnapshot<Timer>> {
            every { timestamp } returns now
            every { meter } returns timer
            every { measurements } returns listOf(
                MeasurementMetric(224.0, Statistic.MEAN),
                MeasurementMetric(178713.0, Statistic.TOTAL_TIME),
                MeasurementMetric(54328.5, Statistic.MAX),
                DistributionMeasurementMetric(2548.7, Statistic.PERCENTILE, 25.0),
                DistributionMeasurementMetric(12548.7, Statistic.PERCENTILE, 99.9)
            )
        }))

        // then
        val result = readRecords("WHERE tenant = 'my-tenant'")
        assertThat(result).all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesMeter(
                    name = "my-meter",
                    timestamp = now,
                    campaign = "my-campaign",
                    scenario = "my-scenario",
                    tags = mapOf(
                        "tag-1" to "value-1",
                        "tag-2" to "value-2",
                        "campaign" to "my-campaign",
                        "scenario" to "my-scenario"
                    ),
                    type = "timer",
                    count = 0L,
                    meanDuration = Duration.parse("PT0.000000224S"),
                    maxDuration = Duration.parse("PT0.000054328S"),
                    sumDuration = Duration.parse("PT0.000178713S"),
                    other = mapOf(
                        "percentile_25.0" to BigDecimal(2548.7),
                        "percentile_99.9" to BigDecimal(12548.7)
                    )
                )
            )
        }
    }

    @Test
    internal fun `should deserialize a gauge`() = testDispatcherProvider.run {
        // given
        val now = Instant.now()
        val gaugeMock = mockk<Gauge> {
            every { id } returns mockk<Meter.Id> {
                every { meterName } returns "my-meter"
                every { tags } returns mapOf(
                    "tenant" to "my-tenant", "campaign" to "my-campaign", "scenario" to "my-scenario",
                    "tag-1" to "value-1", "tag-2" to "value-2"
                )
                every { type } returns MeterType.GAUGE
                every { scenarioName } returns "my-scenario"
                every { campaignKey } returns "my-campaign"
                every { stepName } returns "step tres"
            }
        }
        val gaugeSnapshot = mockk<MeterSnapshot<Gauge>> {
            every { timestamp } returns now
            every { meter } returns gaugeMock
            every { measurements } returns listOf(MeasurementMetric(564.0, Statistic.VALUE))
        }

        // when
        timescaledbMeasurementPublisher.publish(listOf(gaugeSnapshot))

        // then
        val result = readRecords("WHERE tenant = 'my-tenant'")
        assertThat(result).all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesMeter(
                    name = "my-meter",
                    timestamp = now,
                    campaign = "my-campaign",
                    scenario = "my-scenario",
                    tags = mapOf(
                        "campaign" to "my-campaign",
                        "scenario" to "my-scenario",
                        "tag-1" to "value-1",
                        "tag-2" to "value-2"
                    ),
                    type = "gauge",
                    value = BigDecimal("564.000000")
                )
            )
        }
    }

    @Test
    internal fun `should deserialize a counter`() = testDispatcherProvider.run {
        // given
        val now = Instant.now()
        val counterMock = mockk<Counter> {
            every { id } returns mockk<Meter.Id> {
                every { meterName } returns "my-meter"
                every { tags } returns mapOf(
                    "tenant" to "my-tenant", "campaign" to "my-campaign", "scenario" to "my-scenario",
                    "tag-1" to "value-1", "tag-2" to "value-2"
                )
                every { type } returns MeterType.COUNTER
                every { scenarioName } returns "my-scenario"
                every { campaignKey } returns "my-campaign"
                every { stepName } returns "step uno"
            }
        }
        val countSnapshot = mockk<MeterSnapshot<Counter>> {
            every { timestamp } returns now
            every { meter } returns counterMock
            every { measurements } returns listOf(MeasurementMetric(5050.0, Statistic.COUNT))
        }

        // when
        timescaledbMeasurementPublisher.publish(listOf(countSnapshot))

        // then
        val result = readRecords("WHERE tenant = 'my-tenant'")
        assertThat(result).all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesMeter(
                    name = "my-meter",
                    timestamp = now,
                    campaign = "my-campaign",
                    scenario = "my-scenario",
                    tags = mapOf(
                        "campaign" to "my-campaign",
                        "scenario" to "my-scenario",
                        "tag-1" to "value-1",
                        "tag-2" to "value-2"
                    ),
                    type = "counter",
                    count = 5050L
                )
            )
        }
    }

    @Test
    internal fun `should deserialize a distribution summary`() = testDispatcherProvider.run {
        // given
        val now = Instant.now()
        val summaryMock = mockk<DistributionSummary> {
            every { id } returns mockk<Meter.Id> {
                every { meterName } returns "my-meter"
                every { tags } returns mapOf(
                    "tenant" to "my-tenant", "campaign" to "my-campaign", "scenario" to "my-scenario",
                    "tag-1" to "value-1", "tag-2" to "value-2"
                )
                every { type } returns MeterType.DISTRIBUTION_SUMMARY
                every { scenarioName } returns "my-scenario"
                every { campaignKey } returns "my-campaign"
                every { stepName } returns "step quart"
            }
        }
        val summarySnapshot = mockk<MeterSnapshot<DistributionSummary>> {
            every { timestamp } returns now
            every { meter } returns summaryMock
            every { measurements } returns listOf(
                MeasurementMetric(70.0, Statistic.COUNT),
                MeasurementMetric(17873213.0, Statistic.TOTAL),
                MeasurementMetric(548.5, Statistic.MEAN),
                MeasurementMetric(748.5, Statistic.MAX),
                DistributionMeasurementMetric(2548.7, Statistic.PERCENTILE, 25.0),
                DistributionMeasurementMetric(12548.7, Statistic.PERCENTILE, 99.9)
            )
        }
        val snapshot = listOf(summarySnapshot)

        // when
        timescaledbMeasurementPublisher.publish(snapshot)

        // then
        val result = readRecords("WHERE tenant = 'my-tenant'")
        assertThat(result).all {
            hasSize(1)
            index(0).isDataClassEqualTo(
                TimeSeriesMeter(
                    name = "my-meter",
                    timestamp = now,
                    campaign = "my-campaign",
                    scenario = "my-scenario",
                    tags = mapOf(
                        "campaign" to "my-campaign",
                        "scenario" to "my-scenario",
                        "tag-1" to "value-1",
                        "tag-2" to "value-2"
                    ),
                    type = "summary",
                    count = 70L,
                    mean = BigDecimal("548.500000"),
                    max = BigDecimal("748.500000"),
                    sum = BigDecimal("17873213.000000"),
                    other = mapOf(
                        "percentile_25.0" to BigDecimal(2548.7),
                        "percentile_99.9" to BigDecimal(12548.7)
                    )
                )
            )
        }
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
            override val initSchema: Boolean = true
            override val enableSsl: Boolean = false
            override val sslMode: SSLMode = SSLMode.ALLOW
            override val sslRootCert: String? = null
            override val sslCert: String? = null
            override val sslKey: String? = null
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