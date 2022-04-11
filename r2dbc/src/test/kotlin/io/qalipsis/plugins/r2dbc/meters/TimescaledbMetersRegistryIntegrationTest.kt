package io.qalipsis.plugins.r2dbc.meters

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Meter
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

        val meters = mutableListOf<Meter>()

        val firstCounter = Counter.builder("first").register(meterRegistry)
        firstCounter.increment(8.0)

        val secondCounter = Counter.builder("second").register(meterRegistry)
        secondCounter.increment(3.0)

        meters.add(secondCounter)
        meters.add(firstCounter)

        // when
        meterRegistry.publish()

        // then
        val result = connectionPool.create().flatMap {
            val statement = it.createStatement("select count(*) from meters")
            Mono.from(statement.execute())
                .map {
                    it.map { row, _ -> row.get(0) as Long }
                }
                .doOnTerminate { Mono.from(it.close()).subscribe() }
        }.awaitFirstOrNull()?.awaitFirstOrNull()

        assertEquals(2, result)


        val published = connectionPool.create().flatMap {
            val statement = it.createStatement("select * from meters")
            Mono.from(statement.execute())
                .map { it.map { row, _ -> row.get(0) as Long } }
                .doOnTerminate { Mono.from(it.close()).subscribe() }
        }.awaitFirstOrNull()?.awaitFirstOrNull()

        print(published)
//        assertThat(published[0]).isEqualTo("smth")
//        assertThat(published[1]).isEqualTo("smth")

        meterRegistry.stop()
    }
}
