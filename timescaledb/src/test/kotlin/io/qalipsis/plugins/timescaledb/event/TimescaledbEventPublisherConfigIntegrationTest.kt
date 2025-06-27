/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.timescaledb.event

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.events.EventLevel
import io.qalipsis.api.events.EventsPublisher
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration

internal class TimescaledbEventPublisherConfigIntegrationTest {

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
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
    inner class WithPublisher : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start with the publisher`() {
            assertThat(applicationContext.getBeansOfType(EventsPublisher::class.java)).all {
                any { it.isInstanceOf(TimescaledbEventsPublisher::class) }
            }
            val eventsConfig = applicationContext.getBean(TimescaledbEventsPublisherConfiguration::class.java)
            assertThat(eventsConfig).all {
                prop(TimescaledbEventsPublisherConfiguration::host).isEqualTo("localhost")
                prop(TimescaledbEventsPublisherConfiguration::port).isEqualTo(5432)
                prop(TimescaledbEventsPublisherConfiguration::database).isEqualTo("qalipsis")
                prop(TimescaledbEventsPublisherConfiguration::username).isEqualTo("qalipsis_user")
                prop(TimescaledbEventsPublisherConfiguration::password).isEqualTo("qalipsis-pwd")
                prop(TimescaledbEventsPublisherConfiguration::schema).isEqualTo("events")
                prop(TimescaledbEventsPublisherConfiguration::minLevel).isEqualTo(EventLevel.INFO)
                prop(TimescaledbEventsPublisherConfiguration::publishers).isEqualTo(1)
                prop(TimescaledbEventsPublisherConfiguration::batchSize).isEqualTo(20000)
                prop(TimescaledbEventsPublisherConfiguration::lingerPeriod).isEqualTo(Duration.ofSeconds(10))
            }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "events.export.timescaledb.enabled" to StringUtils.TRUE
            )
        }
    }

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
    inner class WithConfiguredPublisher : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start with the configured publisher`() {
            assertThat(applicationContext.getBeansOfType(EventsPublisher::class.java)).all {
                any { it.isInstanceOf(TimescaledbEventsPublisher::class) }
            }
            val eventsConfig = applicationContext.getBean(TimescaledbEventsPublisherConfiguration::class.java)
            assertThat(eventsConfig).all {
                prop(TimescaledbEventsPublisherConfiguration::host).isEqualTo("my-db")
                prop(TimescaledbEventsPublisherConfiguration::port).isEqualTo(12332)
                prop(TimescaledbEventsPublisherConfiguration::database).isEqualTo("the DB")
                prop(TimescaledbEventsPublisherConfiguration::username).isEqualTo("the user")
                prop(TimescaledbEventsPublisherConfiguration::password).isEqualTo("the password")
                prop(TimescaledbEventsPublisherConfiguration::schema).isEqualTo("the schema")
                prop(TimescaledbEventsPublisherConfiguration::minLevel).isEqualTo(EventLevel.DEBUG)
                prop(TimescaledbEventsPublisherConfiguration::publishers).isEqualTo(3)
                prop(TimescaledbEventsPublisherConfiguration::batchSize).isEqualTo(50000)
                prop(TimescaledbEventsPublisherConfiguration::lingerPeriod).isEqualTo(Duration.ofSeconds(3))
            }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "events.export.timescaledb.enabled" to StringUtils.TRUE,
                "events.export.timescaledb.host" to "my-db",
                "events.export.timescaledb.port" to "12332",
                "events.export.timescaledb.database" to "the DB",
                "events.export.timescaledb.username" to "the user",
                "events.export.timescaledb.password" to "the password",
                "events.export.timescaledb.schema" to "the schema",
                "events.export.timescaledb.min-level" to "DEBUG",
                "events.export.timescaledb.publishers" to "3",
                "events.export.timescaledb.batchSize" to "50000",
                "events.export.timescaledb.linger-period" to "3s"
            )
        }
    }
}