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

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.timescaledb.TestUtils.fibonacciFromSize
import io.qalipsis.plugins.timescaledb.event.TimescaledbEvent
import io.qalipsis.plugins.timescaledb.event.TimescaledbEventConverter
import io.qalipsis.plugins.timescaledb.event.TimescaledbEventsPublisher
import io.qalipsis.plugins.timescaledb.event.TimescaledbEventsPublisherConfiguration
import io.qalipsis.plugins.timescaledb.event.catadioptre.publishConvertedEvents
import io.qalipsis.plugins.timescaledb.meter.TimescaledbMeter
import io.qalipsis.plugins.timescaledb.meter.TimescaledbMeasurementPublisher
import io.qalipsis.plugins.timescaledb.meter.TimescaledbMeasurementPublisherFactory
import io.qalipsis.plugins.timescaledb.meter.catadioptre.doPublish
import io.qalipsis.plugins.timescaledb.utils.DbUtils
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.postgresql.client.SSLMode
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
import java.util.concurrent.TimeUnit

/**
 * @author Joël Valère
 */

@Testcontainers
@MicronautTest(startApplication = false, environments = ["standalone", "timescaledb"], transactional = false)
@Timeout(20, unit = TimeUnit.SECONDS)
internal abstract class AbstractTimescaledbTimeSeriesDataProviderIntegrationTest : TestPropertyProvider {

    @Inject
    protected lateinit var measurementPublisherFactory: TimescaledbMeasurementPublisherFactory

    private lateinit var timescaledbMeasurementPublisher: TimescaledbMeasurementPublisher

    @Inject
    private lateinit var eventsConverter: TimescaledbEventConverter

    @Inject
    private lateinit var timescaledbTimeSeriesDataProvider: TimescaledbTimeSeriesDataProvider

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var connection: ConnectionPool

    private lateinit var publisher: TimescaledbEventsPublisher

    private val start: Instant = Instant.parse("2022-08-01T00:00:00Z")

    private val timeStep: Duration = Duration.ofMillis(500)

    abstract val dbPort: Int

    override fun getProperties(): Map<String, String> = mapOf(
        "meters.provider.timescaledb.enabled" to "true",
        "meters.provider.timescaledb.host" to "localhost",
        "meters.provider.timescaledb.port" to "$dbPort",
        "meters.provider.timescaledb.database" to DB_NAME,
        "meters.provider.timescaledb.username" to USERNAME,
        "meters.provider.timescaledb.password" to PASSWORD,
        "meters.provider.timescaledb.schema" to METERS_SCHEMA,

        "meters.export.enabled" to "true",
        "meters.export.timescaledb.enabled" to "true",
        "meters.export.timescaledb.host" to "localhost",
        "meters.export.timescaledb.port" to "$dbPort",
        "meters.export.timescaledb.database" to DB_NAME,
        "meters.export.timescaledb.username" to USERNAME,
        "meters.export.timescaledb.password" to PASSWORD,
        "meters.export.timescaledb.schema" to METERS_SCHEMA,

        "events.provider.timescaledb.enabled" to "true",
        "events.provider.timescaledb.host" to "localhost",
        "events.provider.timescaledb.port" to "$dbPort",
        "events.provider.timescaledb.database" to DB_NAME,
        "events.provider.timescaledb.username" to USERNAME,
        "events.provider.timescaledb.password" to PASSWORD,
        "events.provider.timescaledb.schema" to EVENTS_SCHEMA
    )

    private var metersInitialized = false

    private var eventsInitialized = false

    @BeforeEach
    fun setUpAll() = testDispatcherProvider.run {
        if (!metersInitialized) {
            connection = DbUtils.createConnectionPool(object : DataProviderConfiguration {
                override val host: String = "localhost"
                override val port: Int = dbPort
                override val database: String = DB_NAME
                override val schema: String = METERS_SCHEMA
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

            Flux.usingWhen(
                connection.create(),
                { connection -> Mono.from(connection.createStatement("TRUNCATE TABLE meters").execute()) },
                Connection::close
            ).asFlow().count()

            var currentMeterTimestamp = start - timeStep.multipliedBy(2)
            // Meters of name "my-meter-1" for the tenant 1.
            val meters1Tenant1 = fibonacciFromSize(1, 17).flatMap { value ->
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
            val meters1Tenant2 = fibonacciFromSize(8, 13).flatMap { value ->
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
            val meters1DefaultTenant = fibonacciFromSize(8, 11).flatMap { value ->
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

            timescaledbMeasurementPublisher = measurementPublisherFactory.getPublisher() as TimescaledbMeasurementPublisher
            timescaledbMeasurementPublisher.doPublish(meters1Tenant1 + meters2Tenant1 + meters1Tenant2 + meters1DefaultTenant + meters2DefaultTenant)
            delay(1000) // Wait for the transaction to be fully committed.
            metersInitialized = true
        }

        if (!eventsInitialized) {
            connection = DbUtils.createConnectionPool(object : DataProviderConfiguration {
                override val host: String = "localhost"
                override val port: Int = dbPort
                override val database: String = DB_NAME
                override val schema: String = EVENTS_SCHEMA
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

            Flux.usingWhen(
                connection.create(),
                { connection -> Mono.from(connection.createStatement("TRUNCATE TABLE events").execute()) },
                Connection::close
            ).asFlow().count()

            val configuration = mockk<TimescaledbEventsPublisherConfiguration> {
                every { host } returns "localhost"
                every { port } returns dbPort
                every { database } returns DB_NAME
                every { schema } returns EVENTS_SCHEMA
                every { username } returns USERNAME
                every { password } returns PASSWORD
                every { minLevel } returns EventLevel.TRACE
                every { lingerPeriod } returns Duration.ofNanos(1)
                every { batchSize } returns 2000
                every { publishers } returns 1
                every { enableSsl } returns false
                every { sslMode } returns SSLMode.DISABLE
                every { sslRootCert } returns null
                every { sslCert } returns null
                every { sslKey } returns null
                every { initSchema } returns true
            }

            publisher = TimescaledbEventsPublisher(
                CoroutineScope(SupervisorJob() + Dispatchers.IO),
                configuration,
                eventsConverter
            )
            publisher.start()
            eventsInitialized = true
        }
    }

    @Nested
    inner class DataRetrieval {

        @Test
        internal fun `should fetch space used by a tenant and update its value correctly`() =
            testDispatcherProvider.run {
                // given tenant = tenant-1
                // when + then
                assertThat(timescaledbTimeSeriesDataProvider.retrieveUsedStorage("tenant-1")).isNotNull()
                    .isEqualTo(46917)

                // adding events associated to tenant-1 should update its space used
                // given
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
                publisher.publishConvertedEvents(events1Tenant1 + events2Tenant1)
                delay(1000) // Wait for the transaction to be fully committed.

                // when + then
                assertThat(timescaledbTimeSeriesDataProvider.retrieveUsedStorage("tenant-1")).isNotNull()
                    .isEqualTo(120645)


                // given tenant = tenant-2
                // when + then
                assertThat(timescaledbTimeSeriesDataProvider.retrieveUsedStorage("tenant-2")).isNotNull()
                    .isEqualTo(9681)

                // adding events associated to tenant-2 should update its space used
                // given
                currentEventTimestamp = start - timeStep.multipliedBy(2)
                // Events of name "my-event-1" for the tenant 2.
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
                publisher.publishConvertedEvents(events1Tenant2)
                delay(1000) // Wait for the transaction to be fully committed.

                // when + then
                assertThat(timescaledbTimeSeriesDataProvider.retrieveUsedStorage("tenant-2")).isNotNull()
                    .isEqualTo(24426)


                // given tenant = default-tenant
                // when + then
                assertThat(timescaledbTimeSeriesDataProvider.retrieveUsedStorage("default-tenant")).isNotNull()
                    .isEqualTo(17128)

                // adding events associated to default-tenant should update its space used
                // given
                currentEventTimestamp = start - timeStep.multipliedBy(2)
                // Events of name "my-event-1" for the default tenant.
                val events1DefaultTenant = fibonacciFromSize(3, 12).flatMap { number ->
                    currentEventTimestamp += timeStep
                    listOf(
                        TimescaledbEvent(
                            name = "my-event-1",
                            level = "info",
                            tenant = "default-tenant",
                            campaign = "my-campaign-1",
                            scenario = "my-scenario-1",
                            tags = """{"number-tag": "$number","tenant-tag":"default-tenant","scenario-tag":"my-scenario-1"}""",
                            timestamp = Timestamp.from(currentEventTimestamp),
                            number = number.toBigDecimal(),
                        )
                    )
                }
                // Events of name "my-event-2" for the default tenant.
                currentEventTimestamp = start - timeStep.multipliedBy(2)
                val events2DefaultTenant = fibonacciFromSize(5, 12).flatMap { number ->
                    currentEventTimestamp += timeStep
                    listOf(
                        TimescaledbEvent(
                            name = "my-event-2",
                            level = "info",
                            tenant = "default-tenant",
                            campaign = "my-campaign-1",
                            scenario = "my-scenario-1",
                            tags = """{"number-tag": "$number","tenant-tag":"default-tenant","scenario-tag":"my-scenario-1"}""",
                            timestamp = Timestamp.from(currentEventTimestamp),
                            number = number.toBigDecimal(),
                        )
                    )
                }
                publisher.publishConvertedEvents(events1DefaultTenant + events2DefaultTenant)
                delay(1000) // Wait for the transaction to be fully committed.

                // when + then
                assertThat(timescaledbTimeSeriesDataProvider.retrieveUsedStorage("default-tenant")).isNotNull()
                    .isEqualTo(38193)
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
         * Default meters schema.
         */
        const val METERS_SCHEMA = "meters"

        /**
         * Default events schema.
         */
        const val EVENTS_SCHEMA = "events"

    }
}