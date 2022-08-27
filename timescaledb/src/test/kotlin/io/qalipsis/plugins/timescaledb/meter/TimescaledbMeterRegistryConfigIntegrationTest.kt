package io.qalipsis.plugins.timescaledb.meter

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.prop
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.test.assertk.prop
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class TimescaledbMeterRegistryConfigIntegrationTest {

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
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
    inner class WithRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start with the registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).any {
                it.isInstanceOf(TimescaledbMeterRegistry::class)
            }

            val meterRegistry = applicationContext.getBean(TimescaledbMeterRegistry::class.java)
            assertThat(meterRegistry).prop("config").isNotNull().isInstanceOf(TimescaledbMeterConfig::class).all {
                prop(TimescaledbMeterConfig::host).isEqualTo("localhost")
                prop(TimescaledbMeterConfig::port).isEqualTo(5432)
                prop(TimescaledbMeterConfig::database).isEqualTo("qalipsis")
                prop(TimescaledbMeterConfig::username).isEqualTo("qalipsis_user")
                prop(TimescaledbMeterConfig::password).isEqualTo("qalipsis-pwd")
                prop(TimescaledbMeterConfig::schema).isEqualTo("meters")
                prop(TimescaledbMeterConfig::timestampFieldName).isEqualTo("timestamp")
            }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.timescaledb.enabled" to StringUtils.TRUE,
                "meters.export.timescaledb.autostart" to "false",
                "meters.export.timescaledb.autoconnect" to "false"
            )
        }
    }

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
    inner class WithConfiguredRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start with the configured registry`() {
            assertThat(applicationContext.getBeansOfType(MeterRegistry::class.java)).any {
                it.isInstanceOf(TimescaledbMeterRegistry::class)
            }

            val meterRegistry = applicationContext.getBean(TimescaledbMeterRegistry::class.java)
            assertThat(meterRegistry).prop("config").isNotNull().isInstanceOf(TimescaledbMeterConfig::class).all {
                prop(TimescaledbMeterConfig::host).isEqualTo("my-db")
                prop(TimescaledbMeterConfig::port).isEqualTo(4564)
                prop(TimescaledbMeterConfig::database).isEqualTo("the DB")
                prop(TimescaledbMeterConfig::username).isEqualTo("the user")
                prop(TimescaledbMeterConfig::password).isEqualTo("the password")
                prop(TimescaledbMeterConfig::schema).isEqualTo("the schema")
                prop(TimescaledbMeterConfig::timestampFieldName).isEqualTo("timestamp")
            }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.timescaledb.enabled" to StringUtils.TRUE,
                "meters.export.timescaledb.host" to "my-db",
                "meters.export.timescaledb.port" to "4564",
                "meters.export.timescaledb.database" to "the DB",
                "meters.export.timescaledb.username" to "the user",
                "meters.export.timescaledb.password" to "the password",
                "meters.export.timescaledb.schema" to "the schema",
                "meters.export.timescaledb.autostart" to "false",
                "meters.export.timescaledb.autoconnect" to "false"
            )
        }
    }
}