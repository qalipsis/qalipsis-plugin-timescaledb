package io.qalipsis.plugins.timescaledb.meter

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isBetween
import assertk.assertions.isEqualTo
import assertk.assertions.prop
import io.qalipsis.api.report.query.QueryAggregationOperator
import io.qalipsis.api.report.query.QueryClause
import io.qalipsis.api.report.query.QueryClauseOperator
import io.qalipsis.api.report.query.QueryDescription
import io.qalipsis.plugins.timescaledb.TimescaleDbContainerProvider
import io.qalipsis.plugins.timescaledb.dataprovider.PreparedQuery
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import kotlin.math.pow

@DisabledIfSystemProperty(
    named = "os.arch",
    matches = "aarch.*",
    disabledReason = "The required functions only run in TimescaleDB HA, not supported on ARM"
)
internal class TimescaledbMeterQueryGeneratorIntegrationTest : AbstractMeterQueryGeneratorIntegrationTest() {

    override val dbPort: Int
        get() = db.firstMappedPort

    @Nested
    inner class AggregationCalculation : AbstractMeterQueryGeneratorIntegrationTest.AggregationCalculation() {

        @Test
        internal fun `should calculate the 75% percentile values`() = testDispatcherProvider.run {
            // given
            val query = meterQueryGenerator.prepareQueries(
                "tenant-1", QueryDescription(
                    filters = listOf(QueryClause("name", QueryClauseOperator.IS, "my-meter-1")),
                    fieldName = "value",
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
                    prop(AggregationPoint::result).isBetween(7.92, 7.93)
                }
                index(1).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(2))
                    prop(AggregationPoint::result).isBetween(55.15, 55.16)
                }
                index(2).all {
                    prop(AggregationPoint::bucket).isEqualTo(start + Duration.ofSeconds(4))
                    prop(AggregationPoint::result).isBetween(232.78, 232.79)
                }
            }
        }
    }

    override suspend fun executeSelect(
        query: PreparedQuery,
        start: Instant,
        end: Instant,
        timeframe: Duration?,
        limit: Int?,
        order: String?
    ): List<Map<String, Any?>> {
        // A delay seems necessary for indexation.
        delay(500)
        return super.executeSelect(query, start, end, timeframe, limit, order)
    }

    override suspend fun executeAggregation(
        query: PreparedQuery,
        start: Instant,
        end: Instant,
        timeframe: Duration?
    ): List<AggregationPoint> {
        // A delay seems necessary for indexation.
        delay(500)
        return super.executeAggregation(query, start, end, timeframe)
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