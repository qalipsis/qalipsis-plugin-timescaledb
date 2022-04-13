package io.qalipsis.plugins.r2dbc.meters

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.qalipsis.plugins.r2dbc.config.PostgresqlTemplateTest
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import reactor.core.publisher.Mono
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


@Timeout(1, unit = TimeUnit.MINUTES)
internal class TimescaledbMetersRegistryIntegrationTest : PostgresqlTemplateTest() {

    private lateinit var connectionPool: ConnectionPool

    private lateinit var configuration: TimescaledbMeterConfig

    @BeforeAll
    internal fun setUpAll() {
        val meterRegistryProperties = Properties()
        meterRegistryProperties["timescaledb.username"] = USERNAME
        meterRegistryProperties["timescaledb.password"] = PASSWORD
        meterRegistryProperties["timescaledb.db"] = DB_NAME
        meterRegistryProperties["timescaledb.host"] = "localhost"
        meterRegistryProperties["timescaledb.port"] = "${postgresql.firstMappedPort}"
        meterRegistryProperties["timescaledb.schema"] = "qalipsis_ts"
        meterRegistryProperties["batchSize"] = 2
        configuration = object : TimescaledbMeterConfig() {
            override fun get(key: String?): String? {
                return meterRegistryProperties.getProperty(key)
            }
        }
        connectionPool = ConnectionPool(
            ConnectionPoolConfiguration.builder()
                .connectionFactory(
                    PostgresqlConnectionFactory(
                        PostgresqlConnectionConfiguration.builder()
                            .host("localhost")
                            .username(USERNAME).password(PASSWORD)
                            .database(DB_NAME)
                            .schema("qalipsis_ts")
                            .port(postgresql.firstMappedPort)
                            .build()
                    )
                ).build()
        )
    }

    @Test
    @Timeout(30)
    fun `should export data`() = testDispatcherProvider.run {
        // given
        val meterRegistry = TimescaledbMeterRegistry(configuration, Clock.SYSTEM)
        meterRegistry.start(Executors.defaultThreadFactory())

        val secondCounter = Counter.builder("second").register(meterRegistry)
        secondCounter.increment(3.0)
        val firstCounter = Counter.builder("first").tag("first-tag-key", "first-tag-value").register(meterRegistry)
        firstCounter.increment(8.0)
        val distributionSummary =
            DistributionSummary.builder("summary").tag("summary-tag", "summary-value").register(meterRegistry)

        // when
        meterRegistry.publish()

        // then
        val result = connectionPool.create().flatMap {
            val statement = it.createStatement("select count(*) from meters")
            Mono.from(statement.execute())
                .map { it.map { row, _ -> row.get(0) as Long } }
                .doOnTerminate { Mono.from(it.close()).subscribe() }
        }.awaitFirstOrNull()?.awaitFirstOrNull()

        assertEquals(3, result)

        val counters = connectionPool.create().flatMap {
            val statement = it.createStatement("select count(*) from meters where type='counter'")
            Mono.from(statement.execute())
                .map { it.map { row, _ -> row.get(0) as Long } }
                .doOnTerminate { Mono.from(it.close()).subscribe() }
        }.awaitFirstOrNull()?.awaitFirstOrNull()

        assertEquals(2, counters)

        val summaries = connectionPool.create().flatMap {
            val statement = it.createStatement("select count(*) from meters where type='distribution_summary'")
            Mono.from(statement.execute())
                .map { it.map { row, _ -> row.get(0) as Long } }
                .doOnTerminate { Mono.from(it.close()).subscribe() }
        }.awaitFirstOrNull()?.awaitFirstOrNull()

        assertEquals(1, summaries)

        val summary = connectionPool.create().flatMap {
            val statement =
                it.createStatement("select type, name, tags, timestamp  from meters where type='distribution_summary'")
            Mono.from(statement.execute())
                .map {
                    it.map { row, _ ->
                        TimescaledbMeter(
                            name = row.get(1) as String,
                            type = row.get(0) as String,
                            timestamp = (row.get(3) as LocalDateTime).toInstant(ZoneOffset.UTC),
                            tags = row.get(2) as String
                        )
                    }
                }
                .doOnTerminate { Mono.from(it.close()).subscribe() }
        }.awaitFirstOrNull()?.awaitFirstOrNull()

        assertEquals("distribution_summary", summary!!.type)
        assertEquals("summary", summary.name)
        assertEquals("summary-tag:summary-value, ", summary.tags)

        meterRegistry.stop()
    }
}
