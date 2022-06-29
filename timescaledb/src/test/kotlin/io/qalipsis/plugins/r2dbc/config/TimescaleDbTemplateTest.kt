package io.qalipsis.plugins.r2dbc.config

import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.test.coroutines.TestDispatcherProvider
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import kotlin.math.pow


@Testcontainers
@MicronautTest(environments = ["timescaledb"])
internal abstract class TimescaleDbTemplateTest : TestPropertyProvider {

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    override fun getProperties(): Map<String, String> = mapOf(
        "pgsql.host" to "localhost",
        "pgsql.port" to "${db.firstMappedPort}",
        "pgsql.database" to "$DB_NAME",
        "pgsql.username" to USERNAME,
        "pgsql.username" to PASSWORD
    )

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

        @Container
        @JvmStatic
        val db = TimescaleDbContainerProvider().newInstance().apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(128 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            waitingFor(Wait.forListeningPort())
            withStartupTimeout(Duration.ofSeconds(240))

            withDatabaseName(DB_NAME)
            withUsername(USERNAME)
            withPassword(PASSWORD)
            //withCommand("postgres -c shared_preload_libraries=timescaledb -c log_error_verbosity=VERBOSE -c timescaledb.telemetry_level=OFF")
            withInitScript("pgsql-init.sql")
        }
    }
}