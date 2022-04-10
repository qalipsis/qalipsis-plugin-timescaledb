package io.qalipsis.plugins.r2dbc.meters

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Meter
import io.micronaut.context.BeanContext.run
import io.micronaut.core.naming.conventions.StringConvention
import io.mockk.every
import io.mockk.mockk
import io.qalipsis.api.meters.MetersConfig
import io.qalipsis.plugins.r2dbc.config.PostgresTestContainerConfiguration
import io.qalipsis.plugins.r2dbc.config.PostgresqlTemplateTest
import io.qalipsis.plugins.r2dbc.meters.TimescaledbMeterConfig
import io.qalipsis.plugins.r2dbc.meters.TimescaledbMeterRegistry
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Mono
import java.util.Properties
import java.util.concurrent.Executors

@Testcontainers
internal class TimescaledbMetersRegistryIntegrationTest : PostgresqlTemplateTest() {

    private lateinit var connectionPool: ConnectionPool

    private lateinit var configuration: TimescaledbMeterConfig

    @BeforeAll
    internal fun setUp() {
        val meterRegistryProperties = Properties()
        meterRegistryProperties["timescaledb.username"] = PostgresTestContainerConfiguration.USERNAME
        meterRegistryProperties["timescaledb.password"] = PostgresTestContainerConfiguration.PASSWORD
        meterRegistryProperties["timescaledb.db"] = PostgresTestContainerConfiguration.DB_NAME
        meterRegistryProperties["timescaledb.host"] = "localhost"
        meterRegistryProperties["timescaledb.port"] = "5432"
        meterRegistryProperties["timescaledb.schema"] = "qalipsis"
        meterRegistryProperties["batchSize"] = 2
        configuration = object : TimescaledbMeterConfig() {
            override fun get(key: String?): String? {
                return meterRegistryProperties.getProperty(key)
            }
        }

//        {
//            every { host() } returns "localhost"
//            every { port() } returns "5432"
//            every { db() } returns PostgresTestContainerConfiguration.DB_NAME
//            every { schema() } returns "qalipsis"
//            every { userName() } returns PostgresTestContainerConfiguration.USERNAME
//            every { password() } returns PostgresTestContainerConfiguration.PASSWORD
//            every { batchSize() } returns 2
//        }

//        configuration = mockk {
//            every { host() } returns "localhost"
//            every { port() } returns "5432"
//            every { db() } returns PostgresTestContainerConfiguration.DB_NAME
//            every { schema() } returns "qalipsis"
//            every { userName() } returns PostgresTestContainerConfiguration.USERNAME
//            every { password() } returns PostgresTestContainerConfiguration.PASSWORD
//            every { batchSize() } returns 2
//        }

        connectionPool = ConnectionPool(
            ConnectionPoolConfiguration.builder()
                .connectionFactory(
                    PostgresqlConnectionFactory(
                        PostgresqlConnectionConfiguration.builder().host("localhost")
                            .password(PostgresTestContainerConfiguration.PASSWORD).username(
                                PostgresTestContainerConfiguration.USERNAME
                            )
                            .database(PostgresTestContainerConfiguration.DB_NAME)
                            .schema("qalipsis")
                            .port(pgsqlContainer.getMappedPort(5432))
                            .build()
                    )
                ).build()
        )
    }

    @Test
    @Timeout(30)
    fun `should export data`() = testDispatcherProvider.run{
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
                .map { it.map { row, _ -> row.get(0) as Long } }
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
