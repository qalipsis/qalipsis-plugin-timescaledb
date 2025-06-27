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

package io.qalipsis.plugins.timescaledb.meter

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.prop
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.DistributionMeasurementMetric
import io.qalipsis.api.meters.MeasurementMetric
import io.qalipsis.api.meters.Meter
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.meters.MeterType
import io.qalipsis.api.meters.Statistic
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Connection
import jakarta.inject.Inject
import kotlinx.coroutines.delay
import kotlinx.coroutines.reactive.awaitLast
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Instant
import java.time.OffsetDateTime

@Testcontainers
@WithMockk
@MicronautTest(environments = ["timescaledb"], startApplication = false)
@Timeout(60)
internal abstract class AbstractTimescaledbMeasurementPublisherIntegrationTest : TestPropertyProvider {

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    @Inject
    private lateinit var meterRegistryFactory: TimescaledbMeasurementPublisherFactory

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
            "meters.export.timescaledb.batchSize" to "2",

            "logging.level.io.qalipsis.plugins.timescaledb.meter" to "TRACE"
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

    @Test
    @Timeout(10)
    fun `should export data`() = testDispatcherProvider.run {
        // given
        val measurementPublisher = meterRegistryFactory.getPublisher()

        val now = Instant.now()
        val meterSnapshots = listOf(
            mockk<MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my counter",
                    MeterType.COUNTER,
                    mapOf(
                        "scenario" to "first scenario",
                        "campaign" to "first campaign 5473653",
                        "step" to "step number one"
                    )
                )
                every { measurements } returns listOf(MeasurementMetric(8.0, Statistic.COUNT))
            },
            mockk<MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my gauge",
                    MeterType.GAUGE,
                    mapOf(
                        "scenario" to "third scenario",
                        "campaign" to "third CAMPAIGN 7624839",
                        "step" to "step number three",
                        "foo" to "bar",
                        "any-tag" to "any-value"
                    )
                )
                every { measurements } returns listOf(MeasurementMetric(5.0, Statistic.VALUE))
            }, mockk<MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my timer",
                    MeterType.TIMER,
                    mapOf(
                        "scenario" to "second scenario",
                        "campaign" to "second campaign 47628233",
                        "step" to "step number two",
                    )
                )
                every { measurements } returns listOf(
                    MeasurementMetric(65.0, Statistic.COUNT),
                    MeasurementMetric(224.0, Statistic.MEAN),
                    MeasurementMetric(178713.0, Statistic.TOTAL_TIME),
                    MeasurementMetric(54328.5, Statistic.MAX),
                    DistributionMeasurementMetric(500000448.5, Statistic.PERCENTILE, 85.0),
                    DistributionMeasurementMetric(5432844.5, Statistic.PERCENTILE, 50.0),
                )
            }, mockk<MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my final summary",
                    MeterType.DISTRIBUTION_SUMMARY,
                    mapOf(
                        "scenario" to "fourth scenario",
                        "campaign" to "fourth CAMPAIGN 283239",
                        "step" to "step number four",
                        "dist" to "summary",
                        "local" to "host"
                    )
                )
                every { measurements } returns listOf(
                    MeasurementMetric(70.0, Statistic.COUNT),
                    MeasurementMetric(17873213.0, Statistic.TOTAL),
                    MeasurementMetric(548.5, Statistic.MAX),
                    MeasurementMetric(12.5, Statistic.MEAN),
                    DistributionMeasurementMetric(548.5, Statistic.PERCENTILE, 85.0),
                    DistributionMeasurementMetric(54328.5, Statistic.PERCENTILE, 50.0),
                )
            }, mockk<MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "my Rate",
                    MeterType.RATE,
                    mapOf(
                        "scenario" to "fifth scenario",
                        "campaign" to "campaign 39",
                        "step" to "step number five",
                        "foo" to "bar",
                        "local" to "host"
                    )
                )
                every { measurements } returns listOf(
                    MeasurementMetric(2.0, Statistic.VALUE)
                )
            }, mockk<MeterSnapshot> {
                every { timestamp } returns now
                every { meterId } returns Meter.Id(
                    "throughput",
                    MeterType.THROUGHPUT,
                    mapOf(
                        "scenario" to "sixth scenario",
                        "campaign" to "CEAD@E28339",
                        "step" to "step number six",
                        "a" to "b",
                        "c" to "d"
                    )
                )
                every { measurements } returns listOf(
                    MeasurementMetric(30.0, Statistic.VALUE),
                    MeasurementMetric(22.0, Statistic.MEAN),
                    MeasurementMetric(173.0, Statistic.TOTAL),
                    MeasurementMetric(42.0, Statistic.MAX),
                    DistributionMeasurementMetric(42.0, Statistic.PERCENTILE, 85.0),
                    DistributionMeasurementMetric(30.0, Statistic.PERCENTILE, 50.0),
                )
            })
        measurementPublisher.publish(meterSnapshots)

        // when
        do {
            delay(500)
            val recordsCounts =
                executeSelect("select name from meters ")
            log.info { "Found meters: ${recordsCounts.joinToString { it["name"] as String }}" }
        } while (recordsCounts.size < 6) // One count by meter is expected.
        measurementPublisher.stop()

        // then
        val savedMeters = executeSelect("select * from meters")
            .map { row ->
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
                    max = row["max"] as BigDecimal?,
                    unit = row["unit"] as String?,
                    other = (row["other"] as Json?)?.asString()
                )
            }
        assertThat(savedMeters).all {
            hasSize(6)
            any {
                it.all {
                    prop(TimescaledbMeter::name).isEqualTo("my timer")
                    prop(TimescaledbMeter::tenant).isNull()
                    prop(TimescaledbMeter::campaign).isEqualTo("second campaign 47628233")
                    prop(TimescaledbMeter::scenario).isEqualTo("second scenario")
                    prop(TimescaledbMeter::timestamp).isNotNull()
                    prop(TimescaledbMeter::type).isEqualTo("timer")
                    prop(TimescaledbMeter::tags).isEqualTo("""{"step": "step number two"}""")
                    prop(TimescaledbMeter::value).isNull()
                    prop(TimescaledbMeter::count).isNotNull().transform { it.toInt() }
                        .isEqualTo(65)
                    prop(TimescaledbMeter::max).isNotNull().transform { it.toDouble() }
                        .isEqualTo(54328.5)
                    prop(TimescaledbMeter::mean).isNotNull().transform { it.toInt() }
                        .isEqualTo(224)
                    prop(TimescaledbMeter::sum).isNotNull().transform { it.toInt() }
                        .isEqualTo(178713)
                    prop(TimescaledbMeter::unit).isNotNull().isEqualTo("MICROSECONDS")
                    prop(TimescaledbMeter::other).isNotNull()
                        .isEqualTo("""{"percentile_50.0": "5432844.5", "percentile_85.0": "500000448.5"}""")
                }
            }
            any {
                it.all {
                    prop(TimescaledbMeter::name).isEqualTo("my counter")
                    prop(TimescaledbMeter::tenant).isNull()
                    prop(TimescaledbMeter::campaign).isEqualTo("first campaign 5473653")
                    prop(TimescaledbMeter::scenario).isEqualTo("first scenario")
                    prop(TimescaledbMeter::timestamp).isNotNull()
                    prop(TimescaledbMeter::type).isEqualTo("counter")
                    prop(TimescaledbMeter::tags).isEqualTo("""{"step": "step number one"}""")
                    prop(TimescaledbMeter::value).isNull()
                    prop(TimescaledbMeter::count).isNotNull().transform { it.toInt() }
                        .isEqualTo(8)
                    prop(TimescaledbMeter::max).isNull()
                    prop(TimescaledbMeter::mean).isNull()
                    prop(TimescaledbMeter::sum).isNull()
                    prop(TimescaledbMeter::unit).isNull()
                    prop(TimescaledbMeter::other).isNull()
                }
            }
            any {
                it.all {
                    prop(TimescaledbMeter::name).isEqualTo("my gauge")
                    prop(TimescaledbMeter::tenant).isNull()
                    prop(TimescaledbMeter::campaign).isEqualTo("third CAMPAIGN 7624839")
                    prop(TimescaledbMeter::scenario).isEqualTo("third scenario")
                    prop(TimescaledbMeter::timestamp).isNotNull()
                    prop(TimescaledbMeter::type).isEqualTo("gauge")
                    prop(TimescaledbMeter::tags).isEqualTo("""{"foo": "bar", "step": "step number three", "any-tag": "any-value"}""")
                    prop(TimescaledbMeter::value).isNotNull().transform { it.toInt() }
                        .isEqualTo(5)
                    prop(TimescaledbMeter::count).isNull()
                    prop(TimescaledbMeter::max).isNull()
                    prop(TimescaledbMeter::mean).isNull()
                    prop(TimescaledbMeter::sum).isNull()
                    prop(TimescaledbMeter::unit).isNull()
                    prop(TimescaledbMeter::other).isNull()
                }
            }
            any {
                it.all {
                    prop(TimescaledbMeter::name).isEqualTo("my final summary")
                    prop(TimescaledbMeter::tenant).isNull()
                    prop(TimescaledbMeter::campaign).isEqualTo("fourth CAMPAIGN 283239")
                    prop(TimescaledbMeter::scenario).isEqualTo("fourth scenario")
                    prop(TimescaledbMeter::timestamp).isNotNull()
                    prop(TimescaledbMeter::type).isEqualTo("summary")
                    prop(TimescaledbMeter::tags).isEqualTo("""{"dist": "summary", "step": "step number four", "local": "host"}""")
                    prop(TimescaledbMeter::value).isNull()
                    prop(TimescaledbMeter::count).isNotNull().transform { it.toInt() }
                        .isEqualTo(70)
                    prop(TimescaledbMeter::max).isNotNull().transform { it.toDouble() }
                        .isEqualTo(548.5)
                    prop(TimescaledbMeter::mean).isNotNull().transform { it.toDouble() }
                        .isEqualTo(12.5)
                    prop(TimescaledbMeter::sum).isNotNull().transform { it.toInt() }
                        .isEqualTo(17873213)
                    prop(TimescaledbMeter::unit).isNull()
                    prop(TimescaledbMeter::other).isNotNull()
                        .isEqualTo("""{"percentile_50.0": "54328.5", "percentile_85.0": "548.5"}""")
                }
            }
            any {
                it.all {
                    prop(TimescaledbMeter::name).isEqualTo("my Rate")
                    prop(TimescaledbMeter::tenant).isNull()
                    prop(TimescaledbMeter::campaign).isEqualTo("campaign 39")
                    prop(TimescaledbMeter::scenario).isEqualTo("fifth scenario")
                    prop(TimescaledbMeter::timestamp).isNotNull()
                    prop(TimescaledbMeter::type).isEqualTo("rate")
                    prop(TimescaledbMeter::tags).isEqualTo("""{"foo": "bar", "step": "step number five", "local": "host"}""")
                    prop(TimescaledbMeter::count).isNull()
                    prop(TimescaledbMeter::value).isNotNull().transform { it.toInt() }
                        .isEqualTo(2)
                    prop(TimescaledbMeter::max).isNull()
                    prop(TimescaledbMeter::mean).isNull()
                    prop(TimescaledbMeter::sum).isNull()
                    prop(TimescaledbMeter::unit).isNull()
                    prop(TimescaledbMeter::other).isNull()
                }
            }
            any {
                it.all {
                    prop(TimescaledbMeter::name).isEqualTo("throughput")
                    prop(TimescaledbMeter::tenant).isNull()
                    prop(TimescaledbMeter::campaign).isEqualTo("CEAD@E28339")
                    prop(TimescaledbMeter::scenario).isEqualTo("sixth scenario")
                    prop(TimescaledbMeter::timestamp).isNotNull()
                    prop(TimescaledbMeter::type).isEqualTo("throughput")
                    prop(TimescaledbMeter::tags).isEqualTo("""{"a": "b", "c": "d", "step": "step number six"}""")
                    prop(TimescaledbMeter::count).isNull()
                    prop(TimescaledbMeter::value).isNotNull().transform { it.toInt() }
                        .isEqualTo(30)
                    prop(TimescaledbMeter::max).isNotNull().transform { it.toDouble() }
                        .isEqualTo(42.0)
                    prop(TimescaledbMeter::mean).isNotNull().transform { it.toDouble() }
                        .isEqualTo(22.0)
                    prop(TimescaledbMeter::sum).isNotNull().transform { it.toInt() }
                        .isEqualTo(173)
                    prop(TimescaledbMeter::unit).isNull()
                    prop(TimescaledbMeter::other).isNotNull()
                        .isEqualTo("""{"percentile_50.0": "30", "percentile_85.0": "42"}""")
                }
            }
        }
    }

    private suspend fun executeSelect(statement: String): List<Map<String, *>> {
        return Mono.usingWhen(
            connection.create(),
            { connection ->
                Mono.from(connection.createStatement(statement).execute())
            },
            Connection::close
        ).flatMapMany { result ->
            log.info { "Mapping the result" }
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

        val log = logger()

    }
}
