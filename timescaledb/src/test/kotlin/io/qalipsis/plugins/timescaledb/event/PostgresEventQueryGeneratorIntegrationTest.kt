package io.qalipsis.plugins.timescaledb.event

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.prop
import io.qalipsis.api.report.query.QueryAggregationOperator
import io.qalipsis.api.report.query.QueryClause
import io.qalipsis.api.report.query.QueryClauseOperator
import io.qalipsis.api.report.query.QueryDescription
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.PostgreSQLContainerProvider
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import java.time.Duration
import kotlin.math.pow

internal class PostgresEventQueryGeneratorIntegrationTest : AbstractEventQueryGeneratorIntegrationTest() {

    override val dbPort: Int
        get() = db.firstMappedPort

    @Nested
    inner class AggregationCalculation : AbstractEventQueryGeneratorIntegrationTest.AggregationCalculation() {

        @Test
        internal fun `should calculate the 75% percentile values`() = testDispatcherProvider.run {
            // given
            val query = eventQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-event-1")),
                    fieldName = "number",
                    aggregationOperation = QueryAggregationOperator.PERCENTILE_75,
                    timeframeUnit = Duration.ofSeconds(2)
                )
            )
            val result = executeAggregation(query, start, latestTimestamp - timeStep)

            // then
            assertThat(result).all {
                hasSize(3)
                index(0).all {
                    prop(AggregationPoint::bucket).isEqualTo(start)
                    prop(AggregationPoint::result).isEqualTo(5.0)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isEqualTo(34.0)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isEqualTo(233.0)
                }
            }
        }
    }

    companion object {

        @Container
        @JvmStatic
        val db: JdbcDatabaseContainer<*> = PostgreSQLContainerProvider().newInstance().apply {
            withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withMemory(128 * 1024.0.pow(2).toLong()).withCpuCount(2)
            }
            waitingFor(Wait.forListeningPort())
            withStartupTimeout(Duration.ofSeconds(240))

            withDatabaseName(DB_NAME)
            withUsername(USERNAME)
            withPassword(PASSWORD)
            withInitScript("pgsql-init.sql")
        }
    }
}