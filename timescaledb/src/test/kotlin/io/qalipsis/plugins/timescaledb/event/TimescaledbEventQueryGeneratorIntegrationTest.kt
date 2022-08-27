package io.qalipsis.plugins.timescaledb.event

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isBetween
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.prop
import io.qalipsis.api.query.QueryAggregationOperator
import io.qalipsis.api.query.QueryClause
import io.qalipsis.api.query.QueryClauseOperator
import io.qalipsis.api.query.QueryDescription
import io.qalipsis.api.report.TimeSeriesAggregationResult
import io.qalipsis.plugins.timescaledb.TimescaleDbContainerProvider
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import kotlin.math.pow

@DisabledIfSystemProperty(
    named = "os.arch",
    matches = "aarch.*",
    disabledReason = "The required functions only run in TimescaleDB HA, not supported on ARM"
)
internal class TimescaledbEventQueryGeneratorIntegrationTest : AbstractEventQueryGeneratorIntegrationTest() {

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
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start)
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                        .isBetween(7.92, 7.93)
                }
                index(1).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(2))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                        .isBetween(55.15, 55.16)
                }
                index(2).all {
                    prop(TimeSeriesAggregationResult::start).isEqualTo(start + Duration.ofSeconds(4))
                    prop(TimeSeriesAggregationResult::value).isNotNull().transform { it.toDouble() }
                        .isBetween(232.78, 232.79)
                }
            }
        }
    }

    companion object {

        @Container
        @JvmStatic
        val db: JdbcDatabaseContainer<*> = TimescaleDbContainerProvider()
            // The image timescaledb-ha is required for the hyperfunctions but is not compliant with ARM architectures.
            .newInstance(DockerImageName.parse("timescale/timescaledb-ha").withTag("pg14-latest"))
            .apply {
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(128 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
                waitingFor(Wait.forListeningPort())
                withStartupTimeout(Duration.ofSeconds(240))

                withDatabaseName(DB_NAME)
                withUsername(USERNAME)
                withPassword(PASSWORD)
                withCommand("postgres -c shared_preload_libraries=timescaledb -c log_error_verbosity=VERBOSE -c timescaledb.telemetry_level=OFF -c max_connections=100")
                withInitScript("pgsql-init-timescaledb.sql")
            }
    }
}