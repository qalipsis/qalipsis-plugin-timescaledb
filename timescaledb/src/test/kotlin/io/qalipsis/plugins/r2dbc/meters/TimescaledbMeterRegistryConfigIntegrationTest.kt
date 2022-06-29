package io.qalipsis.plugins.r2dbc.meters

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.prop
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.PropertySource
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class TimescaledbMeterRegistryConfigIntegrationTest {

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
    @Property(name = "meters.timescaledb.enabled", value = StringUtils.FALSE)
    inner class WithoutRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start without the registry`() {
            assertThat(applicationContext.getBeansOfType(TimescaledbMeterRegistry::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
    @Property(name = "meters.timescaledb.enabled", value = StringUtils.TRUE)
    inner class WithRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start with the registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).all {
                any { it.isInstanceOf(TimescaledbMeterRegistry::class) }
            }
            val meterRegistryConfig =
                (applicationContext.getBeansOfType(TimescaledbMeterRegistry::class.java) as ArrayList)[0].config
            assertThat(meterRegistryConfig).all {
                prop(TimescaledbMeterConfig::host).isEqualTo("localhost")
                prop(TimescaledbMeterConfig::port).isEqualTo(5432)
                prop(TimescaledbMeterConfig::database).isEqualTo("qalipsis")
                prop(TimescaledbMeterConfig::userName).isEqualTo("qalipsis_user")
                prop(TimescaledbMeterConfig::password).isEqualTo("qalipsis-pwd")
                prop(TimescaledbMeterConfig::schema).isEqualTo("meters")
                prop(TimescaledbMeterConfig::timestampFieldName).isEqualTo("timestamp")
            }
        }
    }

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
    @PropertySource(
        Property(name = "meters.timescaledb.enabled", value = StringUtils.TRUE),
        Property(name = "meters.timescaledb.host", value = "my-db"),
        Property(name = "meters.timescaledb.port", value = "123432"),
        Property(name = "meters.timescaledb.database", value = "the DB"),
        Property(name = "meters.timescaledb.username", value = "the user"),
        Property(name = "meters.timescaledb.password", value = "the password"),
        Property(name = "meters.timescaledb.schema", value = "the schema")
    )
    inner class WithConfiguredRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start with the configured registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).all {
                any { it.isInstanceOf(TimescaledbMeterRegistry::class) }
            }
            val meterRegistryConfig = applicationContext.getBean(TimescaledbMeterRegistry::class.java)
            assertThat(meterRegistryConfig.config).all {
                prop(TimescaledbMeterConfig::host).isEqualTo("my-db")
                prop(TimescaledbMeterConfig::port).isEqualTo(123432)
                prop(TimescaledbMeterConfig::database).isEqualTo("the DB")
                prop(TimescaledbMeterConfig::userName).isEqualTo("the user")
                prop(TimescaledbMeterConfig::password).isEqualTo("the password")
                prop(TimescaledbMeterConfig::schema).isEqualTo("the schema")
                prop(TimescaledbMeterConfig::timestampFieldName).isEqualTo("timestamp")
            }
        }
    }
}