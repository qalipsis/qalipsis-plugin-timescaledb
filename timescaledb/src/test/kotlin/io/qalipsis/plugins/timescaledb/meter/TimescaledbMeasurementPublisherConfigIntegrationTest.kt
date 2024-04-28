/*
 * Copyright 2024 AERIS IT Solutions GmbH
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

package io.qalipsis.plugins.timescaledb.meter

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.meters.MeasurementPublisherFactory
import io.qalipsis.test.assertk.prop
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class TimescaledbMeasurementPublisherConfigIntegrationTest {

    @Nested
    @MicronautTest(environments = ["timescaledb"], startApplication = false)
    inner class WithoutRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should not start without the right configurations`() {
            assertThat(applicationContext.getBeansOfType(TimescaledbMeasurementPublisher::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).isEmpty()
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
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).any {
                it.isInstanceOf(TimescaledbMeasurementPublisherFactory::class)
                    .prop(TimescaledbMeasurementPublisherFactory::getPublisher).all {
                        isInstanceOf(TimescaledbMeasurementPublisher::class)
                    }
            }

            val meterRegistry = applicationContext.getBean(TimescaledbMeasurementPublisherFactory::class.java)
                .getPublisher() as TimescaledbMeasurementPublisher
            assertThat(meterRegistry).prop("config").isNotNull().isInstanceOf(TimescaledbMeterConfig::class).all {
                prop(TimescaledbMeterConfig::host).isEqualTo("localhost")
                prop(TimescaledbMeterConfig::port).isEqualTo(5432)
                prop(TimescaledbMeterConfig::database).isEqualTo("qalipsis")
                prop(TimescaledbMeterConfig::username).isEqualTo("qalipsis_user")
                prop(TimescaledbMeterConfig::password).isEqualTo("qalipsis-pwd")
                prop(TimescaledbMeterConfig::schema).isEqualTo("meters")
                prop(TimescaledbMeterConfig::timestampFieldName).isEqualTo("timestamp")
            }

            val meterRegistryFactory =
                applicationContext.getBean(MeasurementPublisherFactory::class.java) as TimescaledbMeasurementPublisherFactory
            var generatedMeasurementPublisher = meterRegistryFactory.getPublisher()
            assertThat(generatedMeasurementPublisher).all {
                isInstanceOf(TimescaledbMeasurementPublisher::class).prop("config").isNotNull()
                    .isInstanceOf(TimescaledbMeterConfig::class)
                    .all {
                        prop(TimescaledbMeterConfig::host).isEqualTo("localhost")
                        prop(TimescaledbMeterConfig::port).isEqualTo(5432)
                        prop(TimescaledbMeterConfig::database).isEqualTo("qalipsis")
                        prop(TimescaledbMeterConfig::username).isEqualTo("qalipsis_user")
                        prop(TimescaledbMeterConfig::password).isEqualTo("qalipsis-pwd")
                        prop(TimescaledbMeterConfig::schema).isEqualTo("meters")
                        prop(TimescaledbMeterConfig::timestampFieldName).isEqualTo("timestamp")
                    }

                assertk.assertThat(generatedMeasurementPublisher).all {
                    isInstanceOf(TimescaledbMeasurementPublisher::class).prop("config").isNotNull()
                        .isInstanceOf(TimescaledbMeterConfig::class)
                        .all {
                            prop(TimescaledbMeterConfig::host).isEqualTo("localhost")
                            prop(TimescaledbMeterConfig::port).isEqualTo(5432)
                            prop(TimescaledbMeterConfig::database).isEqualTo("qalipsis")
                            prop(TimescaledbMeterConfig::username).isEqualTo("qalipsis_user")
                            prop(TimescaledbMeterConfig::password).isEqualTo("qalipsis-pwd")
                            prop(TimescaledbMeterConfig::schema).isEqualTo("meters")
                            prop(TimescaledbMeterConfig::timestampFieldName).isEqualTo("timestamp")
                        }
                }
            }

        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.timescaledb.enabled" to "true",
                "meters.export.enabled" to "true",
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
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).any {
                it.isInstanceOf(TimescaledbMeasurementPublisherFactory::class)
                    .prop(TimescaledbMeasurementPublisherFactory::getPublisher).all {
                        isInstanceOf(TimescaledbMeasurementPublisher::class)
                    }
            }

            val measurementPublisher = applicationContext.getBean(TimescaledbMeasurementPublisherFactory::class.java)
                .getPublisher() as TimescaledbMeasurementPublisher
            assertThat(measurementPublisher).prop("config").isNotNull().isInstanceOf(TimescaledbMeterConfig::class)
                .all {
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
                "meters.export.timescaledb.enabled" to "true",
                "meters.export.enabled" to "true",
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