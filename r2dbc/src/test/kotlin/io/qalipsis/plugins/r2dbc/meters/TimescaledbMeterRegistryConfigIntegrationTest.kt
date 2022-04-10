package io.qalipsis.plugins.r2dbc.meters

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.prop
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.qalipsis.plugins.r2dbc.config.PostgresqlTemplateTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class TimescaledbMeterRegistryConfigIntegrationTest : PostgresqlTemplateTest() {

    @Nested
    @MicronautTest(propertySources = ["classpath:application-timescaledb.yml"])
    inner class WithRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(10)
        fun `should start with the registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).all {
                hasSize(1)
                any { it.isInstanceOf(TimescaledbMeterRegistry::class) }
            }
            val meterRegistryConfig =
                (applicationContext.getBeansOfType(TimescaledbMeterRegistry::class.java) as ArrayList).get(0).config
            assertThat(meterRegistryConfig).all {
                prop(TimescaledbMeterConfig::prefix).isEqualTo("timescaledb")
                prop(TimescaledbMeterConfig::port).isEqualTo("5432")
                prop(TimescaledbMeterConfig::timestampFieldName).isEqualTo("timestamp")
                prop(TimescaledbMeterConfig::userName).isEqualTo("qalipsis")
                prop(TimescaledbMeterConfig::password).isEqualTo("qalipsis")
                prop(TimescaledbMeterConfig::db).isEqualTo("qalipsis")
                prop(TimescaledbMeterConfig::host).isEqualTo("localhost")
                prop(TimescaledbMeterConfig::schema).isEqualTo("qalipsis")

            }
        }
    }
}