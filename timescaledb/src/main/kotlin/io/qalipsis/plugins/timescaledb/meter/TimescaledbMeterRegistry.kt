package io.qalipsis.plugins.timescaledb.meter

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.step.StepMeterRegistry
import io.micrometer.core.instrument.util.NamedThreadFactory
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.timescaledb.dataprovider.setBigDecimalOrNull
import io.qalipsis.plugins.timescaledb.dataprovider.setIntOrNull
import io.qalipsis.plugins.timescaledb.dataprovider.setStringOrNull
import io.qalipsis.plugins.timescaledb.liquibase.LiquibaseConfiguration
import io.qalipsis.plugins.timescaledb.liquibase.LiquibaseRunner
import org.postgresql.Driver
import java.time.Instant
import java.util.concurrent.TimeUnit

internal class TimescaledbMeterRegistry(
    private val config: TimescaledbMeterConfig,
    private val converter: TimescaledbMeterConverter,
    clock: Clock
) : StepMeterRegistry(config, clock) {

    private lateinit var datasource: HikariDataSource

    private val sqlInsertStatement = String.format(SQL, "${config.schema()}.meters")

    private var schemaInitialized = false

    init {
        if (config.autostart() || config.autoconnect()) {
            createOrUpdateSchemaIfRequired()

            val poolConfig = HikariConfig()
            poolConfig.isAutoCommit = true
            poolConfig.schema = config.schema()
            poolConfig.username = config.username()
            poolConfig.password = config.password()
            poolConfig.driverClassName = Driver::class.java.canonicalName
            // See https://jdbc.postgresql.org/documentation/80/connect.html
            poolConfig.jdbcUrl = "jdbc:postgresql://${config.host()}:${config.port()}/${config.database()}"

            poolConfig.minimumIdle = 1
            poolConfig.maximumPoolSize = 1

            datasource = HikariDataSource(poolConfig)
        }

        if (config.autostart()) {
            super.start(DEFAULT_THREAD_FACTORY)
        }
    }

    private fun createOrUpdateSchemaIfRequired() {
        if (!schemaInitialized) {
            LiquibaseRunner(
                LiquibaseConfiguration(
                    changeLog = "db/liquibase-meters-changelog.xml",
                    host = config.host(),
                    port = config.port(),
                    username = config.username(),
                    password = config.password(),
                    database = config.database(),
                    defaultSchemaName = config.schema()
                )
            ).run()
            schemaInitialized = true
        }
    }

    override fun stop() {
        super.stop()
        datasource.close()
        log.debug { "The meter registry publisher was stopped" }

    }

    override fun getBaseTimeUnit(): TimeUnit {
        return TimeUnit.NANOSECONDS
    }

    public override fun publish() {
        val timescaledbMeters =
            converter.convert(meters, Instant.ofEpochMilli(clock.wallTime()), config().namingConvention(), baseTimeUnit)
        doPublish(timescaledbMeters)
    }

    @KTestable
    private fun doPublish(timescaledbMeters: List<TimescaledbMeter>) {
        if (log.isTraceEnabled) {
            log.debug {
                "Meters to publish: ${
                    timescaledbMeters.joinToString(
                        prefix = "\n\t",
                        separator = "\n\t"
                    ) { it.toString() }
                }"
            }
        } else {
            log.debug { "${timescaledbMeters.size} meters are to be published" }
        }
        tryAndLogOrNull(log) {
            datasource.connection.use { connection ->
                val results = connection.prepareStatement(sqlInsertStatement).use { statement ->
                    var bindIndex: Int
                    timescaledbMeters.forEach { meters ->
                        bindIndex = 1
                        statement.setString(bindIndex++, meters.name)
                        statement.setStringOrNull(bindIndex++, meters.tags)
                        statement.setTimestamp(bindIndex++, meters.timestamp)
                        statement.setStringOrNull(bindIndex++, meters.tenant)
                        statement.setStringOrNull(bindIndex++, meters.campaign)
                        statement.setStringOrNull(bindIndex++, meters.scenario)
                        statement.setString(bindIndex++, meters.type)
                        statement.setBigDecimalOrNull(bindIndex++, meters.count)
                        statement.setBigDecimalOrNull(bindIndex++, meters.value)
                        statement.setBigDecimalOrNull(bindIndex++, meters.sum)
                        statement.setBigDecimalOrNull(bindIndex++, meters.mean)
                        statement.setIntOrNull(bindIndex++, meters.activeTasks)
                        statement.setBigDecimalOrNull(bindIndex++, meters.duration)
                        statement.setStringOrNull(bindIndex++, meters.unit)
                        statement.setBigDecimalOrNull(bindIndex++, meters.max)
                        statement.setStringOrNull(bindIndex++, meters.other)

                        statement.addBatch()
                    }
                    statement.executeBatch()
                }
                val updatedRows = results.count { it >= 1 }
                log.debug { "$updatedRows meters were successfully published" }
            }
        }
    }

    private companion object {

        val DEFAULT_THREAD_FACTORY = NamedThreadFactory("timescaledb-metrics-publisher")

        const val SQL =
            "INSERT into %s (name, tags, timestamp, tenant, campaign, scenario, type, count, value, sum, mean, active_tasks, duration, unit, max, other) values (?, to_json(?::json), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        val log = logger()
    }
}