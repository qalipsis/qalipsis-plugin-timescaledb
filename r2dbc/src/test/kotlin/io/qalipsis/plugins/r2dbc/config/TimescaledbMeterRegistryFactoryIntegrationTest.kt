package io.qalipsis.plugins.r2dbc.config

import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isNotEmpty
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.plugins.r2dbc.meters.TimescaledbMeterRegistry
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class TimescaledbMeterRegistryFactoryIntegrationTest : PostgresqlTemplateTest() {

    @Nested
    @MicronautTest(startApplication = false)
    inner class WithoutMeters : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.enabled" to "false",
                "meters.timescaledb.enabled" to "true",
                "meters.timescaledb.password" to "qalipsis",
                "meters.timescaledb.username" to "qalipsis",
                "meters.bootstrap.port" to pgsqlContainer.getMappedPort(5432).toString()
            )
        }

        @Test
        @Timeout(10)
        internal fun `shouldn't start without meters enabled property`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(TimescaledbMeterRegistry::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(startApplication = false)
    inner class WithMetersButWithoutTimescaledb : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.enabled" to "true",
                "meters.timescaledb.enabled" to "false",
                "meters.timescaledb.password" to "qalipsis",
                "meters.timescaledb.username" to "qalipsis",
                "meters.bootstrap.servers" to pgsqlContainer.getMappedPort(5432).toString()
            )
        }

        @Test
        @Timeout(10)
        internal fun `shouldn't start without timescaledb meters enabled property`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(TimescaledbMeterRegistry::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(startApplication = false)
    inner class WithTimescaledbMeterRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        override fun getProperties(): MutableMap<String, String> {
            return mutableMapOf(
                "meters.enabled" to "true",
                "meters.timescaledb.enabled" to "true",
                "meters.timescaledb.password" to "qalipsis",
                "meters.timescaledb.username" to "qalipsis",
                "meters.bootstrap.servers" to pgsqlContainer.getMappedPort(5432).toString()
            )
        }

        @Test
        @Timeout(10)
        internal fun `should start with timescaledb meter registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).isNotEmpty()
            assertThat(applicationContext.getBeansOfType(TimescaledbMeterRegistry::class.java)).hasSize(1)
        }
    }
}