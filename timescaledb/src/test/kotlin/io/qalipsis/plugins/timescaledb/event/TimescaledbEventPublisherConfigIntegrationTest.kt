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
import io.qalipsis.plugins.timescaledb.event.TimescaledbEventsPublisher
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