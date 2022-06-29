package io.qalipsis.plugins.r2dbc.events

import io.qalipsis.plugins.r2dbc.config.TimescaleDbContainerProvider
import io.qalipsis.plugins.r2dbc.config.TimescaleDbTemplateTest
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import java.time.Duration
import kotlin.math.pow


internal class TimescaledbEventsPublisherIntegrationTest : AbstractTimescaledbEventsPublisherIntegrationTest() {

    override val dbPort: Int
        get() = db.firstMappedPort

    companion object {

        @Container
        @JvmStatic
        val db: JdbcDatabaseContainer<*> = TimescaleDbContainerProvider().newInstance().apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(128 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            waitingFor(Wait.forListeningPort())
            withStartupTimeout(Duration.ofSeconds(240))

            withDatabaseName(TimescaleDbTemplateTest.DB_NAME)
            withUsername(TimescaleDbTemplateTest.USERNAME)
            withPassword(TimescaleDbTemplateTest.PASSWORD)
            withCommand("postgres -c shared_preload_libraries=timescaledb -c log_error_verbosity=VERBOSE -c timescaledb.telemetry_level=OFF")
            withInitScript("pgsql-init.sql")
        }
    }

}
