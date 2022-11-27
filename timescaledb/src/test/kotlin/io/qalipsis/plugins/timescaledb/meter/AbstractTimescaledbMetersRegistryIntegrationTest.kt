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
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.prop
import io.micrometer.core.instrument.Tag
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.postgresql.codec.Json
import jakarta.inject.Inject
import kotlinx.coroutines.delay
import kotlinx.coroutines.reactive.awaitLast
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Duration
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger

@Testcontainers
@MicronautTest(environments = ["timescaledb"], startApplication = false)
@WithMockk
@Timeout(60)
internal abstract class AbstractTimescaledbMetersRegistryIntegrationTest : TestPropertyProvider {

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    @Inject
    private lateinit var meterRegistry: TimescaledbMeterRegistry

    private lateinit var connection: ConnectionPool

    abstract val dbPort: Int

    override fun getProperties(): Map<String, String> {
        return mapOf(
            "meters.export.enabled" to "true",

            "meters.export.timescaledb.enabled" to "true",
            "meters.export.timescaledb.username" to USERNAME,
            "meters.export.timescaledb.password" to PASSWORD,
            "meters.export.timescaledb.database" to DB_NAME,
            "meters.export.timescaledb.host" to "localhost",
            "meters.export.timescaledb.port" to "$dbPort",
            "meters.export.timescaledb.schema" to "meters",
            "meters.export.timescaledb.step" to "1s",
            "meters.export.timescaledb.batchSize" to "2"
        )
    }

    @BeforeAll
    internal fun setUpAll() {
        connection = ConnectionPool(
            ConnectionPoolConfiguration.builder()
                .connectionFactory(
                    PostgresqlConnectionFactory(
                        PostgresqlConnectionConfiguration.builder()
                            .host("localhost")
                            .username(USERNAME).password(PASSWORD)
                            .database(DB_NAME)
                            .schema("meters")
                            .port(dbPort)
                            .build()
                    )
                ).build()
        )
    }

    @AfterEach
    internal fun tearDown() {
        meterRegistry.clear()
    }

    @Test
    @Timeout(20)
    fun `should export data`() = testDispatcherProvider.run {
        // given
        meterRegistry.timer("1-the-timer").apply {
            record(Duration.ofMillis(12))
            record(Duration.ofMillis(8))
        }
        meterRegistry.counter(
            "2-the-counter",
            "first-tag-key",
            "first-tag-value",
            "tenant",
            "tenant-1",
            "campaign",
            "campaign-1",
            "scenario",
            "scenario-1"
        ).apply {
            increment(8.80)
            increment(2.40)
        }
        meterRegistry.summary(
            "3-the-summary",
            "summary-tag",
            "summary-value",
            "tenant",
            "tenant-2",
            "campaign",
            "campaign-2",
            "scenario",
            "scenario-2"
        ).apply {
            record(130.60)
            record(110.40)
            record(90.20)
        }
        meterRegistry.gauge(
            "4-the-gauge",
            listOf(
                Tag.of("gauge-tag", "gauge-value"),
                Tag.of("tenant", "tenant-3"),
                Tag.of("campaign", "campaign-3"),
                Tag.of("scenario", "scenario-3")
            ),
            AtomicInteger(13)
        )!!.apply {
            incrementAndGet()
            addAndGet(6)
        }

        // when
        do {
            delay(500)
            val recordsCounts =
                executeSelect("select name, count(*) from meters where count > 0 or value > 2 group by name order by name")
            logger().info { "Found meters: ${recordsCounts.joinToString { it["name"] as String }}" }
        } while (recordsCounts.size < 4) // One count by meter is expected.
        meterRegistry.stop()

        // then
        val savedMeters = executeSelect(
            """
            select meters.* 
                from meters, (SELECT name, min(timestamp) as timestamp from meters where count > 0 or value > 2 group by name) t 
                WHERE meters.name = t.name and meters.timestamp = t.timestamp order by meters.name
            """.trimIndent()
        ).map { row ->
            TimescaledbMeter(
                name = row["name"] as String,
                type = row["type"] as String,
                timestamp = Timestamp.from((row["timestamp"] as OffsetDateTime).toInstant()),
                tenant = row["tenant"] as String?,
                campaign = row["campaign"] as String?,
                scenario = row["scenario"] as String?,
                tags = (row["tags"] as Json?)?.asString(),
                count = row["count"] as BigDecimal?,
                value = row["value"] as BigDecimal?,
                sum = row["sum"] as BigDecimal?,
                mean = row["mean"] as BigDecimal?,
                activeTasks = row["active_tasks"] as Int?,
                duration = row["duration"] as BigDecimal?,
                max = row["max"] as BigDecimal?,
                other = row["other"] as String?
            )
        }
        assertThat(savedMeters).all {
            hasSize(4)
            index(0).all {
                prop(TimescaledbMeter::name).isEqualTo("1-the-timer")
                prop(TimescaledbMeter::timestamp).isNotNull()
                prop(TimescaledbMeter::type).isEqualTo("timer")
                prop(TimescaledbMeter::tags).isNull()
                prop(TimescaledbMeter::count).isNotNull().transform { it.toInt() }.isEqualTo(2)
                prop(TimescaledbMeter::max).isNotNull().transform { it.toLong() }
                    .isEqualTo(Duration.ofMillis(12).toNanos())
                prop(TimescaledbMeter::mean).isNotNull().transform { it.toLong() }
                    .isEqualTo(Duration.ofMillis(10).toNanos())
                prop(TimescaledbMeter::sum).isNotNull().transform { it.toLong() }
                    .isEqualTo(Duration.ofMillis(20).toNanos())
                prop(TimescaledbMeter::tenant).isNull()
                prop(TimescaledbMeter::campaign).isNull()
                prop(TimescaledbMeter::scenario).isNull()
            }
            index(1).all {
                prop(TimescaledbMeter::name).isEqualTo("2-the-counter")
                prop(TimescaledbMeter::timestamp).isNotNull()
                prop(TimescaledbMeter::type).isEqualTo("counter")
                prop(TimescaledbMeter::tags).isEqualTo("""{"first-tag-key": "first-tag-value"}""")
                prop(TimescaledbMeter::count).isNotNull().transform { it.toDouble() }.isEqualTo(11.2)
                prop(TimescaledbMeter::tenant).isEqualTo("tenant-1")
                prop(TimescaledbMeter::campaign).isEqualTo("campaign-1")
                prop(TimescaledbMeter::scenario).isEqualTo("scenario-1")
            }
            index(2).all {
                prop(TimescaledbMeter::name).isEqualTo("3-the-summary")
                prop(TimescaledbMeter::timestamp).isNotNull()
                prop(TimescaledbMeter::type).isEqualTo("distribution_summary")
                prop(TimescaledbMeter::tags).isEqualTo("""{"summary-tag": "summary-value"}""")
                prop(TimescaledbMeter::count).isNotNull().transform { it.toDouble() }.isEqualTo(3.0)
                prop(TimescaledbMeter::max).isNotNull().transform { it.toDouble() }.isEqualTo(130.6)
                prop(TimescaledbMeter::mean).isNotNull().transform { it.toDouble() }.isEqualTo(110.4)
                prop(TimescaledbMeter::sum).isNotNull().transform { it.toDouble() }.isEqualTo(331.2)
                prop(TimescaledbMeter::tenant).isEqualTo("tenant-2")
                prop(TimescaledbMeter::campaign).isEqualTo("campaign-2")
                prop(TimescaledbMeter::scenario).isEqualTo("scenario-2")
            }
            index(3).all {
                prop(TimescaledbMeter::name).isEqualTo("4-the-gauge")
                prop(TimescaledbMeter::timestamp).isNotNull()
                prop(TimescaledbMeter::type).isEqualTo("gauge")
                prop(TimescaledbMeter::tags).isEqualTo("""{"gauge-tag": "gauge-value"}""")
                prop(TimescaledbMeter::value).isNotNull().transform { it.toDouble() }.isEqualTo(20.0)
                prop(TimescaledbMeter::tenant).isEqualTo("tenant-3")
                prop(TimescaledbMeter::campaign).isEqualTo("campaign-3")
                prop(TimescaledbMeter::scenario).isEqualTo("scenario-3")
            }
        }
    }

    private suspend fun executeSelect(statement: String): List<Map<String, *>> {
        return connection.create()
            .flatMap { connection ->
                Mono.from(connection.createStatement(statement).execute())
            }.flatMapMany { result ->
                result.map { row, rowMetadata -> rowMetadata.columnMetadatas.associate { it.name to row[it.name] } }
            }.collectList().awaitLast()
    }

    companion object {

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

    }
}
