package io.qalipsis.plugins.r2dbc.events

import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.PostgreSQLContainerProvider
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import java.time.Duration
import kotlin.math.pow


internal class PostgresEventsPublisherIntegrationTest : AbstractTimescaledbEventsPublisherIntegrationTest() {

    override val dbPort: Int
        get() = db.firstMappedPort

    companion object {

        @Container
        @JvmStatic
        val db: JdbcDatabaseContainer<*> = PostgreSQLContainerProvider().newInstance().apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(50 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            waitingFor(Wait.forListeningPort())
            withStartupTimeout(Duration.ofSeconds(60))

            withDatabaseName(DB_NAME)
            withUsername(USERNAME)
            withPassword(PASSWORD)
            withInitScript("pgsql-init.sql")
        }
    }

}