package io.qalipsis.plugins.r2dbc.config

import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.plugins.r2dbc.config.PostgresTestContainerConfiguration.DB_NAME
import io.qalipsis.plugins.r2dbc.config.PostgresTestContainerConfiguration.testProperties
import io.qalipsis.test.coroutines.TestDispatcherProvider
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers


@Testcontainers
@MicronautTest(environments = ["timescaledb"])
internal abstract class PostgresqlTemplateTest : TestPropertyProvider {

    @JvmField
    @RegisterExtension
    protected val testDispatcherProvider = TestDispatcherProvider()

    override fun getProperties() = pgsqlContainer.testProperties()

    fun getHost() = "jdbc:postgresql://localhost:${pgsqlContainer.getMappedPort(5432)}/$DB_NAME"

    companion object {

        @Container
        @JvmField
        val pgsqlContainer = PostgresTestContainerConfiguration.createContainer()

    }
}