package io.qalipsis.plugins.r2dbc.meters

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.FunctionTimer
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.TimeGauge
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.step.StepMeterRegistry
import io.micrometer.core.instrument.util.StringEscapeUtils
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.r2dbc.liquibase.LiquibaseConfiguration
import io.qalipsis.plugins.r2dbc.liquibase.LiquibaseRunner
import io.qalipsis.plugins.r2dbc.setBigDecimalOrNull
import io.qalipsis.plugins.r2dbc.setIntOrNull
import io.qalipsis.plugins.r2dbc.setStringOrNull
import org.postgresql.Driver
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

internal class TimescaledbMeterRegistry(
    val config: TimescaledbMeterConfig, clock: Clock
) : StepMeterRegistry(config, clock) {

    private lateinit var datasource: HikariDataSource

    private lateinit var sqlInsertStatement: String

    override fun start(threadFactory: ThreadFactory) {
        val host = config.get("timescaledb.host")
        val port = config.get("timescaledb.port").toInt()
        val username = config.get("timescaledb.username")
        val password = config.get("timescaledb.password")
        val database = config.get("timescaledb.database")
        val defaultSchemaName = config.get("timescaledb.schema")
        LiquibaseRunner(
            LiquibaseConfiguration(
                changeLog = "db/liquibase-meters-changelog.xml",
                host = host,
                port = port,
                username = username,
                password = password,
                database = database,
                defaultSchemaName = defaultSchemaName
            )
        ).run()

        val poolConfig = HikariConfig()
        poolConfig.isAutoCommit = true
        poolConfig.schema = defaultSchemaName
        poolConfig.username = username
        poolConfig.password = password
        poolConfig.driverClassName = Driver::class.java.canonicalName
        // See https://jdbc.postgresql.org/documentation/80/connect.html
        poolConfig.jdbcUrl = "jdbc:postgresql://${host}:${port}/${database}"

        poolConfig.minimumIdle = 1
        poolConfig.maximumPoolSize = 1

        datasource = HikariDataSource(poolConfig)
        sqlInsertStatement = String.format(SQL, "${defaultSchemaName}.meters")

        super.start(threadFactory)
    }

    override fun stop() {
        super.stop()
        datasource.close()
        log.debug { "The meter registry publisher was stopped" }

    }

    override fun getBaseTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    public override fun publish() {
        val timestamp = Timestamp.from(Instant.ofEpochMilli(clock.wallTime()))
        val timescaledbMeters = meters.mapNotNull { meter ->
            val tags = getConventionTags(meter.id)
            val serializedTags = if (tags.isNotEmpty()) {
                tags.joinToString(
                    ",",
                    prefix = "{",
                    postfix = "}"
                ) { tag -> """"${StringEscapeUtils.escapeJson(tag.key)}":"${StringEscapeUtils.escapeJson(tag.value)}"""" }
            } else {
                null
            }

            val timescaledbMeter = TimescaledbMeter(
                timestamp = timestamp,
                type = meter.id.type.toString().lowercase(),
                name = getConventionName(meter.id),
                tags = serializedTags
            )
            when (meter) {
                is TimeGauge -> convertTimeGauge(meter, timescaledbMeter)
                is Gauge -> convertGauge(meter, timescaledbMeter)
                is Counter -> convertCounter(meter, timescaledbMeter)
                is Timer -> convertTimer(meter, timescaledbMeter)
                is DistributionSummary -> convertSummary(meter, timescaledbMeter)
                is LongTaskTimer -> convertLongTaskTimer(meter, timescaledbMeter)
                is FunctionCounter -> convertFunctionCounter(meter, timescaledbMeter)
                is FunctionTimer -> convertFunctionTimer(meter, timescaledbMeter)
                else -> convertMeter(meter, timescaledbMeter)
            }
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
                        statement.setString(bindIndex++, meters.type)
                        statement.setBigDecimalOrNull(bindIndex++, meters.count)
                        statement.setBigDecimalOrNull(bindIndex++, meters.value)
                        statement.setBigDecimalOrNull(bindIndex++, meters.sum)
                        statement.setBigDecimalOrNull(bindIndex++, meters.mean)
                        statement.setIntOrNull(bindIndex++, meters.activeTasks)
                        statement.setBigDecimalOrNull(bindIndex++, meters.duration)
                        statement.setBigDecimalOrNull(bindIndex++, meters.max)
                        statement.setStringOrNull(bindIndex++, meters.other)

                        statement.addBatch()
                    }
                    statement.executeBatch()
                }
                val updatedRows = results.count { it > 1 }
                log.debug { "$updatedRows meters were successfully published" }
            }
        }
    }

    /**
     * Timescaledb converter for Counter.
     */
    private fun convertCounter(counter: Counter, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        return convertCounter(counter.count(), timescaledbMeter)
    }

    /**
     * Timescaledb converter for FunctionCounter.
     */
    private fun convertFunctionCounter(counter: FunctionCounter, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        return convertCounter(counter.count(), timescaledbMeter)
    }

    /**
     * Timescaledb converter for Counter with value.
     */
    private fun convertCounter(value: Double, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        if (java.lang.Double.isFinite(value)) {
            return timescaledbMeter.copy(count = BigDecimal(value))
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for Gauge.
     */
    private fun convertGauge(gauge: Gauge, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val value = gauge.value()
        if (java.lang.Double.isFinite(value)) {
            return timescaledbMeter.copy(value = BigDecimal(value))
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for TimeGauge.
     */
    private fun convertTimeGauge(gauge: TimeGauge, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val value = gauge.value(baseTimeUnit)
        if (java.lang.Double.isFinite(value)) {
            return timescaledbMeter.copy(value = BigDecimal(value))
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for FunctionTimer.
     */
    private fun convertFunctionTimer(timer: FunctionTimer, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val sum = timer.totalTime(baseTimeUnit)
        val mean = timer.mean(baseTimeUnit)
        return timescaledbMeter.copy(count = BigDecimal(timer.count()), sum = BigDecimal(sum), mean = BigDecimal(mean))
    }

    /**
     * Timescaledb converter for LongTaskTimer.
     */
    private fun convertLongTaskTimer(timer: LongTaskTimer, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        return timescaledbMeter.copy(
            activeTasks = timer.activeTasks(),
            duration = BigDecimal(timer.duration(baseTimeUnit))
        )
    }

    /**
     * Timescaledb converter for Timer.
     */
    private fun convertTimer(timer: Timer, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        return timescaledbMeter.copy(
            count = BigDecimal(timer.count().toDouble()),
            sum = BigDecimal(timer.totalTime(baseTimeUnit)),
            mean = BigDecimal(timer.mean(baseTimeUnit)),
            max = BigDecimal(timer.max(baseTimeUnit))
        )
    }

    /**
     * Timescaledb serializer for DistributionSummary.
     */
    private fun convertSummary(summary: DistributionSummary, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val histogramSnapshot = summary.takeSnapshot()
        return timescaledbMeter.copy(
            count = BigDecimal(histogramSnapshot.count().toDouble()),
            sum = BigDecimal(histogramSnapshot.total()),
            mean = BigDecimal(histogramSnapshot.mean()),
            max = BigDecimal(histogramSnapshot.max())
        )
    }

    /**
     * Timescaledb further converter for previous kinds of Meter
     */
    private fun convertMeter(meter: Meter, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val measurements = meter.measure()
        val names = mutableListOf<String>()
        // Snapshot values should be used throughout this method as there are chances for values to be changed in-between.
        val values = mutableListOf<Double>()
        for (measurement in measurements) {
            val value = measurement.value
            if (!java.lang.Double.isFinite(value)) {
                continue
            }
            names.add(measurement.statistic.tagValueRepresentation)
            values.add(value)
        }
        return if (names.isEmpty()) {
            timescaledbMeter
        } else {
            val otherForSave = StringBuilder("")
            for (i in names.indices) {
                otherForSave.append(names[i]).append(":").append(values[i]).append(", ")
            }
            timescaledbMeter.copy(other = otherForSave.toString())
        }
    }

    private companion object {

        const val SQL =
            "INSERT into %s (name, tags, timestamp, type, count, value, sum, mean, active_tasks, duration, max, other) values (?, to_json(?::json), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        val log = logger()
    }
}