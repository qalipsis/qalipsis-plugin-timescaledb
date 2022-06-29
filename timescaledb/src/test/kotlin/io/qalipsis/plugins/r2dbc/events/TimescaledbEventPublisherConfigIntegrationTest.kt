package io.qalipsis.plugins.r2dbc.events

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.PropertySource
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventsPublisher
import io.qalipsis.plugins.r2dbc.config.TimescaledbEventsConfiguration
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration

internal class TimescaledbEventPublisherConfigIntegrationTest {

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
    @Property(name = "events.export.timescaledb.enabled", value = StringUtils.FALSE)
    inner class WithoutPublisher {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start without the registry`() {
            assertThat(applicationContext.getBeansOfType(TimescaledbEventsPublisher::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
    @Property(name = "events.export.timescaledb.enabled", value = StringUtils.TRUE)
    inner class WithPublisher {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start with the publisher`() {
            assertThat(applicationContext.getBeansOfType(EventsPublisher::class.java)).all {
                any { it.isInstanceOf(TimescaledbEventsPublisher::class) }
            }
            val eventsConfig = applicationContext.getBean(TimescaledbEventsConfiguration::class.java)
            assertThat(eventsConfig).all {
                prop(TimescaledbEventsConfiguration::host).isEqualTo("localhost")
                prop(TimescaledbEventsConfiguration::port).isEqualTo(5432)
                prop(TimescaledbEventsConfiguration::database).isEqualTo("qalipsis")
                prop(TimescaledbEventsConfiguration::username).isEqualTo("qalipsis_user")
                prop(TimescaledbEventsConfiguration::password).isEqualTo("qalipsis-pwd")
                prop(TimescaledbEventsConfiguration::schema).isEqualTo("events")
                prop(TimescaledbEventsConfiguration::minLevel).isEqualTo(EventLevel.INFO)
                prop(TimescaledbEventsConfiguration::publishers).isEqualTo(1)
                prop(TimescaledbEventsConfiguration::batchSize).isEqualTo(20000)
                prop(TimescaledbEventsConfiguration::lingerPeriod).isEqualTo(Duration.ofSeconds(10))
            }
        }
    }

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
    @PropertySource(
        Property(name = "events.export.timescaledb.enabled", value = StringUtils.TRUE),
        Property(name = "events.export.timescaledb.host", value = "my-db"),
        Property(name = "events.export.timescaledb.port", value = "123432"),
        Property(name = "events.export.timescaledb.database", value = "the DB"),
        Property(name = "events.export.timescaledb.username", value = "the user"),
        Property(name = "events.export.timescaledb.password", value = "the password"),
        Property(name = "events.export.timescaledb.schema", value = "the schema"),
        Property(name = "events.export.timescaledb.min-level", value = "DEBUG"),
        Property(name = "events.export.timescaledb.publishers", value = "3"),
        Property(name = "events.export.timescaledb.batchSize", value = "50000"),
        Property(name = "events.export.timescaledb.linger-period", value = "3s")
    )
    inner class WithConfiguredPublisher {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start with the configured publisher`() {
            assertThat(applicationContext.getBeansOfType(EventsPublisher::class.java)).all {
                any { it.isInstanceOf(TimescaledbEventsPublisher::class) }
            }
            val eventsConfig = applicationContext.getBean(TimescaledbEventsConfiguration::class.java)
            assertThat(eventsConfig).all {
                prop(TimescaledbEventsConfiguration::host).isEqualTo("my-db")
                prop(TimescaledbEventsConfiguration::port).isEqualTo(123432)
                prop(TimescaledbEventsConfiguration::database).isEqualTo("the DB")
                prop(TimescaledbEventsConfiguration::username).isEqualTo("the user")
                prop(TimescaledbEventsConfiguration::password).isEqualTo("the password")
                prop(TimescaledbEventsConfiguration::schema).isEqualTo("the schema")
                prop(TimescaledbEventsConfiguration::minLevel).isEqualTo(EventLevel.DEBUG)
                prop(TimescaledbEventsConfiguration::publishers).isEqualTo(3)
                prop(TimescaledbEventsConfiguration::batchSize).isEqualTo(50000)
                prop(TimescaledbEventsConfiguration::lingerPeriod).isEqualTo(Duration.ofSeconds(3))
            }
        }
    }
}