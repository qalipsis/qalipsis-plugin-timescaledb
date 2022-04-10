package io.qalipsis.plugins.r2dbc.config

import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import kotlin.math.pow

internal object PostgresTestContainerConfiguration {

    /**
     * Default image name and tag.
     */
    private const val DEFAULT_DOCKER_IMAGE = "postgres:14.1-alpine"

    /**
     * Default db name.
     */
    const val DB_NAME = "qalipsis_db"

    /**
     * Default username.
     */
    const val USERNAME = "qalipsis_user"

    /**
     * Default password.
     */
    const val PASSWORD = "qalipsis-pwd"

    fun PostgreSQLContainer<*>.testProperties(): Map<String, String> = mapOf(
        "datasources.test.url" to "jdbc:postgresql://localhost:${firstMappedPort}/$DB_NAME",
        "datasources.test.username" to USERNAME,
        "datasources.test.password" to PASSWORD,
        "datasources.test.dialect" to "POSTGRES",
        "datasources.test.driverClassName" to "org.postgresql.Driver",
        "datasources.test.schema-generate" to "NONE",
        "logging.level.io.micronaut.data.query" to "TRACE"
    )

    @JvmStatic
    fun createContainer(imageNameAndTag: String = DEFAULT_DOCKER_IMAGE): PostgreSQLContainer<*> {
        val result = PostgreSQLContainer<Nothing>(imageNameAndTag)
            .apply {
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
        result.start()
        return result
    }
}