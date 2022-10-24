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

package io.qalipsis.plugins.timescaledb.meter

import assertk.all
import assertk.assertThat
import assertk.assertions.containsOnly
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.key
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.timescaledb.meter.catadioptre.doPublish
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.Connection
import jakarta.inject.Inject
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.TimeUnit

@Testcontainers
@MicronautTest(environments = ["timescaledb", "head"], startApplication = false)
@Timeout(1, unit = TimeUnit.MINUTES)
internal abstract class AbstractMeterDataProviderIntegrationTest : TestPropertyProvider {

    @Inject
    private lateinit var meterDataProvider: TimescaledbMeterDataProvider

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var connection: PostgresqlConnectionFactory

    @Inject
    protected lateinit var meterRegistry: TimescaledbMeterRegistry

    abstract val dbPort: Int

    override fun getProperties(): Map<String, String> = mapOf(
        "meters.provider.timescaledb.enabled" to "true",
        "meters.provider.timescaledb.host" to "localhost",
        "meters.provider.timescaledb.port" to "$dbPort",
        "meters.provider.timescaledb.database" to DB_NAME,
        "meters.provider.timescaledb.username" to USERNAME,
        "meters.provider.timescaledb.password" to PASSWORD,
        "meters.provider.timescaledb.schema" to SCHEMA,

        "meters.export.enabled" to "true",
        "meters.export.timescaledb.enabled" to "true",
        "meters.export.timescaledb.host" to "localhost",
        "meters.export.timescaledb.port" to "$dbPort",
        "meters.export.timescaledb.database" to DB_NAME,
        "meters.export.timescaledb.username" to USERNAME,
        "meters.export.timescaledb.password" to PASSWORD,
        "meters.export.timescaledb.schema" to SCHEMA
    )

    @BeforeEach
    fun setUpAll() {
        connection = PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host("localhost").port(dbPort)
                .username(USERNAME)
                .password(PASSWORD)
                .database(DB_NAME)
                .schema(SCHEMA)
                .build()
        )
    }

    @AfterEach
    fun tearDown() {
        Flux.usingWhen(
            connection.create(),
            { connection -> Mono.from(connection.createStatement("truncate table meters").execute()) },
            Connection::close
        ).blockLast()
    }

    @Test
    @Timeout(20)
    internal fun `should list the names without filter`() = testDispatcherProvider.run {
        // given
        // Index of constant size are used to make the verification of the alpha sorting easier.
        meterRegistry.doPublish((100..199).flatMap {
            listOf(
                // In the tenant 1, meters are saved twice to verify the distinct.
                TimescaledbMeter(
                    "my-meter-$it-gauge",
                    timestamp = Timestamp.from(Instant.now()),
                    type = "gauge",
                    tenant = "tenant-1",
                    campaign = "any",
                    tags = null
                ),
                TimescaledbMeter(
                    "my-meter-$it-gauge",
                    timestamp = Timestamp.from(Instant.now()),
                    type = "gauge",
                    tenant = "tenant-1",
                    campaign = "any",
                    tags = null
                ),
                TimescaledbMeter(
                    "my-meter-$it-timer",
                    timestamp = Timestamp.from(Instant.now()),
                    type = "timer",
                    tenant = "tenant-2",
                    campaign = "any",
                    tags = null
                )
            )
        })

        // when
        val allNamesOfTenant1 = meterDataProvider.searchNames("tenant-1", emptySet(), 200)

        // then
        assertThat(allNamesOfTenant1.toList()).all {
            hasSize(100)
            (0..99).forEach { index ->
                index(index).isEqualTo("my-meter-${100 + index}-gauge")
            }
        }

        // when
        val someNamesOfTenant2 = meterDataProvider.searchNames("tenant-2", emptySet(), 30)

        // then
        assertThat(someNamesOfTenant2.toList()).all {
            hasSize(30)
            (0..29).forEach { index ->
                index(index).isEqualTo("my-meter-${100 + index}-timer")
            }
        }
    }

    @Test
    @Timeout(20)
    internal fun `should list the names with filter`() = testDispatcherProvider.run {
        // given
        // Index of constant size are used to make the verification of the alpha sorting easier.
        meterRegistry.doPublish((100..199).flatMap {
            listOf(
                TimescaledbMeter(
                    "my-meter-$it-gauge",
                    timestamp = Timestamp.from(Instant.now()),
                    type = "gauge",
                    tenant = "tenant-1",
                    campaign = "any",
                    tags = null
                ),
                TimescaledbMeter(
                    "my-meter-$it-timer",
                    timestamp = Timestamp.from(Instant.now()),
                    type = "timer",
                    tenant = "tenant-2",
                    campaign = "any",
                    tags = null
                )
            )
        })
        val filters = setOf("mY-mEteR-10*", "*-1?9-*")

        // when
        var result = meterDataProvider.searchNames("tenant-1", filters, 20)

        // then
        assertThat(result).all {
            hasSize(19)
            containsOnly(
                "my-meter-100-gauge",
                "my-meter-101-gauge",
                "my-meter-102-gauge",
                "my-meter-103-gauge",
                "my-meter-104-gauge",
                "my-meter-105-gauge",
                "my-meter-106-gauge",
                "my-meter-107-gauge",
                "my-meter-108-gauge",
                "my-meter-109-gauge",
                "my-meter-119-gauge",
                "my-meter-129-gauge",
                "my-meter-139-gauge",
                "my-meter-149-gauge",
                "my-meter-159-gauge",
                "my-meter-169-gauge",
                "my-meter-179-gauge",
                "my-meter-189-gauge",
                "my-meter-199-gauge"
            )
        }

        // when
        result = meterDataProvider.searchNames("tenant-2", filters, 5)

        // then
        assertThat(result).all {
            hasSize(5)
            containsOnly(
                "my-meter-100-timer",
                "my-meter-101-timer",
                "my-meter-102-timer",
                "my-meter-103-timer",
                "my-meter-104-timer"
            )
        }
    }


    @Test
    @Timeout(20)
    internal fun `should list the tags without filters`() = testDispatcherProvider.run {
        // given
        meterRegistry.doPublish(
            listOf(
                TimescaledbMeter(
                    RandomStringUtils.randomAlphabetic(5),
                    timestamp = Timestamp.from(Instant.now()),
                    type = "gauge",
                    tenant = "tenant-1",
                    campaign = "any",
                    tags = """{"tag-1":"value-1"}"""
                ),
                TimescaledbMeter(
                    RandomStringUtils.randomAlphabetic(5),
                    timestamp = Timestamp.from(Instant.now()),
                    type = "gauge",
                    tenant = "tenant-1",
                    campaign = "any",
                    tags = """{"tag-1":"value-1","tag-2":"value-2","tag-3":""}"""
                ),
                TimescaledbMeter(
                    RandomStringUtils.randomAlphabetic(5),
                    timestamp = Timestamp.from(Instant.now()),
                    type = "gauge",
                    tenant = "tenant-1",
                    campaign = "any",
                    tags = """{"tag-1":"value-2","tag-2":"value-2","tag-3":"value-3"}"""
                ),
                TimescaledbMeter(
                    RandomStringUtils.randomAlphabetic(5),
                    timestamp = Timestamp.from(Instant.now()),
                    type = "type",
                    tenant = "tenant-2",
                    campaign = "any",
                    tags = """{"tag-2":"value-3"}"""
                )
            )
        )

        // when
        val allTagsOfTenant1 = meterDataProvider.searchTagsAndValues("tenant-1", emptySet(), 200)

        // then
        assertThat(allTagsOfTenant1).all {
            hasSize(3)
            key("tag-1").all {
                hasSize(2)
                containsOnly("value-1", "value-2")
            }
            key("tag-2").all {
                hasSize(1)
                containsOnly("value-2")
            }
            key("tag-3").all {
                hasSize(1)
                containsOnly("value-3")
            }
        }

        // when
        val allTagsOfTenant2 = meterDataProvider.searchTagsAndValues("tenant-2", emptySet(), 200)

        // then
        assertThat(allTagsOfTenant2).all {
            hasSize(1)
            key("tag-2").containsOnly("value-3")
        }

        // when
        val someTagsOfTenant1 = meterDataProvider.searchTagsAndValues("tenant-1", emptySet(), 2)

        // then
        assertThat(someTagsOfTenant1).all {
            hasSize(2)
            key("tag-1").containsOnly("value-1", "value-2")
            key("tag-2").containsOnly("value-2")
        }
    }

    @Test
    @Timeout(20)
    internal fun `should list the tags with filter`() = testDispatcherProvider.run {
        // given
        meterRegistry.doPublish(
            listOf(
                TimescaledbMeter(
                    RandomStringUtils.randomAlphabetic(5),
                    timestamp = Timestamp.from(Instant.now()),
                    type = "gauge",
                    tenant = "tenant-1",
                    campaign = "any",
                    tags = """{"tag-1":"value-1"}"""
                ),
                TimescaledbMeter(
                    RandomStringUtils.randomAlphabetic(5),
                    timestamp = Timestamp.from(Instant.now()),
                    type = "gauge",
                    tenant = "tenant-1",
                    campaign = "any",
                    tags = """{"tag-1":"value-1","tag-2":"value-2","tag-3":""}"""
                ),
                TimescaledbMeter(
                    RandomStringUtils.randomAlphabetic(5),
                    timestamp = Timestamp.from(Instant.now()),
                    type = "gauge",
                    tenant = "tenant-1",
                    campaign = "any",
                    tags = """{"tag-1":"value-2","tag-2":"value-2","tag-3":"value-3"}"""
                ),
                TimescaledbMeter(
                    RandomStringUtils.randomAlphabetic(5),
                    timestamp = Timestamp.from(Instant.now()),
                    type = "type",
                    tenant = "tenant-2",
                    campaign = "any",
                    tags = """{"tag-2":"value-3"}"""
                )
            )
        )

        // when
        var result = meterDataProvider.searchTagsAndValues("tenant-1", setOf("tag-?"), 200)

        // then
        assertThat(result).all {
            hasSize(3)
            key("tag-1").all {
                hasSize(2)
                containsOnly("value-1", "value-2")
            }
            key("tag-2").all {
                hasSize(1)
                containsOnly("value-2")
            }
            key("tag-3").all {
                hasSize(1)
                containsOnly("value-3")
            }
        }

        // when
        result = meterDataProvider.searchTagsAndValues("tenant-1", setOf("*-2"), 200)

        // then
        assertThat(result).all {
            hasSize(2)
            key("tag-1").containsOnly("value-2")
            key("tag-2").containsOnly("value-2")
        }

        // when
        result = meterDataProvider.searchTagsAndValues("tenant-1", setOf("*g-2"), 200)

        // then
        assertThat(result).all {
            hasSize(1)
            key("tag-2").containsOnly("value-2")
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
         * Default schema.
         */
        const val SCHEMA = "meters"

    }
}