package io.qalipsis.plugins.r2dbc.meters

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.prop
import io.micrometer.core.instrument.Clock
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.spyk
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.reactive.awaitLast
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
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger


@Testcontainers
@MicronautTest(environments = ["timescaledb"], startApplication = false)
@Timeout(60)
internal abstract class AbstractTimescaledbMetersRegistryIntegrationTest {

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    private lateinit var connection: ConnectionPool

    private lateinit var configuration: TimescaledbMeterConfig

    abstract val dbPort: Int

    @BeforeAll
    internal fun setUpAll() {
        val meterRegistryProperties = Properties()
        meterRegistryProperties["timescaledb.username"] = USERNAME
        meterRegistryProperties["timescaledb.password"] = PASSWORD
        meterRegistryProperties["timescaledb.database"] = DB_NAME
        meterRegistryProperties["timescaledb.host"] = "localhost"
        meterRegistryProperties["timescaledb.port"] = "$dbPort"
        meterRegistryProperties["timescaledb.schema"] = "meters"
        meterRegistryProperties["timescaledb.step"] = "500ms"
        meterRegistryProperties["timescaledb.batchSize"] = 2
        configuration = object : TimescaledbMeterConfig() {
            override fun get(key: String?): String? {
                return meterRegistryProperties.getProperty(key)
            }
        }
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
    @Timeout(30)
    fun `should export data`() = testDispatcherProvider.run {
        // given
        val meterRegistry = spyk(TimescaledbMeterRegistry(configuration, Clock.SYSTEM), recordPrivateCalls = true)
        val latch = SuspendedCountLatch(1)
        every { meterRegistry.publish() } answers {
            callOriginal()
            latch.blockingDecrement()
        }
        meterRegistry.start(Executors.defaultThreadFactory())
        meterRegistry.timer("1-the-timer").apply {
            record(Duration.ofMillis(12))
            record(Duration.ofMillis(8))
        }
        meterRegistry.counter("2-the-counter", "first-tag-key", "first-tag-value").apply {
            increment(8.0)
            increment(2.0)
        }
        meterRegistry.summary("3-the-summary", "summary-tag", "summary-value").apply {
            record(130.0)
            record(110.0)
            record(90.0)
        }
        meterRegistry.gauge("4-the-gauge", AtomicInteger())!!.addAndGet(654)

        // when
        latch.await()
        meterRegistry.stop()

        // then
        val savedMeters = executeSelect("select * from meters order by name").map { row ->
            TimescaledbMeter(
                name = row["name"] as String,
                type = row["type"] as String,
                timestamp = Timestamp.from((row["timestamp"] as OffsetDateTime).toInstant()),
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
                prop(TimescaledbMeter::max).isNotNull().transform { it.toDouble() }.isEqualTo(12.0)
                prop(TimescaledbMeter::mean).isNotNull().transform { it.toDouble() }.isEqualTo(10.0)
                prop(TimescaledbMeter::sum).isNotNull().transform { it.toDouble() }.isEqualTo(20.0)
            }
            index(1).all {
                prop(TimescaledbMeter::name).isEqualTo("2-the-counter")
                prop(TimescaledbMeter::timestamp).isNotNull()
                prop(TimescaledbMeter::type).isEqualTo("counter")
                prop(TimescaledbMeter::tags).isEqualTo("""{"first-tag-key": "first-tag-value"}""")
                prop(TimescaledbMeter::count).isNotNull().transform { it.toDouble() }.isEqualTo(10.0)
            }
            index(2).all {
                prop(TimescaledbMeter::name).isEqualTo("3-the-summary")
                prop(TimescaledbMeter::timestamp).isNotNull()
                prop(TimescaledbMeter::type).isEqualTo("distribution_summary")
                prop(TimescaledbMeter::tags).isEqualTo("""{"summary-tag": "summary-value"}""")
                prop(TimescaledbMeter::count).isNotNull().transform { it.toInt() }.isEqualTo(3)
                prop(TimescaledbMeter::max).isNotNull().transform { it.toDouble() }.isEqualTo(130.0)
                prop(TimescaledbMeter::mean).isNotNull().transform { it.toDouble() }.isEqualTo(110.0)
                prop(TimescaledbMeter::sum).isNotNull().transform { it.toDouble() }.isEqualTo(330.0)
            }
            index(3).all {
                prop(TimescaledbMeter::name).isEqualTo("4-the-gauge")
                prop(TimescaledbMeter::timestamp).isNotNull()
                prop(TimescaledbMeter::type).isEqualTo("gauge")
                prop(TimescaledbMeter::tags).isNull()
                prop(TimescaledbMeter::value).isNotNull().transform { it.toDouble() }.isEqualTo(654.0)
            }
        }
    }

    private suspend fun executeSelect(statement: String): List<Map<String, *>> {
        return connection.create()
            .flatMap { connection ->
                Mono.from(connection.createStatement(statement).execute())
            }.flatMapMany { result ->
                result.map { row, rowMetadata -> rowMetadata.columnNames.associateWith { name -> row[name] } }
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
