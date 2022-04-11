package io.qalipsis.plugins.r2dbc.meters

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.FunctionTimer
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.TimeGauge
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.step.StepMeterRegistry
import io.micrometer.core.instrument.util.StringEscapeUtils
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.SuspendedCountLatch
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.Statement
import kotlinx.coroutines.reactive.awaitLast
import kotlinx.coroutines.runBlocking
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.Locale
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

internal class TimescaledbMeterRegistry(val config: TimescaledbMeterConfig, clock: Clock) :
    StepMeterRegistry(config, clock) {
    private lateinit var databaseClient: ConnectionPool
    private lateinit var publicationLatch: SuspendedCountLatch

    override fun start(threadFactory: ThreadFactory) {
        databaseClient = ConnectionPool(
            ConnectionPoolConfiguration.builder()
                .connectionFactory(
                    PostgresqlConnectionFactory(
                        PostgresqlConnectionConfiguration.builder().host(config.get("timescaledb.host"))
                            .password(config.get("timescaledb.password")).username(config.get("timescaledb.username"))
                            .database(config.get("timescaledb.db"))
                            .schema(config.get("timescaledb.schema"))
                            .port(config.get("timescaledb.port").toInt())
                            .build()
                    )
                ).build()
        )
        publicationLatch = SuspendedCountLatch(0)
        super.start(threadFactory)
    }

    override fun stop() {
        super.stop()
        runBlocking {
            log.debug { "Waiting for ${publicationLatch.get()} publication jobs to be completed" }
            publicationLatch.await()
        }
        log.debug { "The events logger was stopped" }
        tryAndLogOrNull(log) {
            databaseClient.close()
        }
    }

    override fun getBaseTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    public override fun publish() {
        try {
            val timescaledbMeters = meters.map {
                val timestamp = generateTimestamp()
                val name = getName(it)
                val type = it.id.type.toString().lowercase(Locale.getDefault())
                val tags = getTags(it)
                val tagsForSave = StringBuilder("")
                for (tag in tags) {
                    tagsForSave.append(StringEscapeUtils.escapeJson(tag.key)).append(":")
                        .append(StringEscapeUtils.escapeJson(tag.value)).append(", ")
                }
                val timescaledbMeter =
                    TimescaledbMeter(timestamp = timestamp, type = type, name = name, tags = tagsForSave.toString())
                when (it) {
                    is TimeGauge -> convertTimeGauge(it, timescaledbMeter)
                    is Gauge -> convertGauge(it, timescaledbMeter)
                    is Counter -> convertCounter(it, timescaledbMeter)
                    is Timer -> convertTimer(it, timescaledbMeter)
                    is DistributionSummary -> convertSummary(it, timescaledbMeter)
                    is LongTaskTimer -> convertLongTaskTimer(it, timescaledbMeter)
                    is FunctionCounter -> convertFunctionCounter(it, timescaledbMeter)
                    is FunctionTimer -> convertFunctionTimer(it, timescaledbMeter)
                    else -> convertMeter(it, timescaledbMeter)
                }
            }
            runBlocking {
                databaseClient.create().flatMap {
                    val statement = it.createStatement(
                        """
                    insert into meters (timestamp, type, count, value, sum, mean, active_tasks, duration, max, name, tags, other, id) 
                    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, nextval('meters_seq'))"""
                    )

                    timescaledbMeters.mapIndexed { index, meter ->
                        if (meter != null) {
                            statement.bind(0, meter.timestamp).bind(1, meter.type)
                                .bind(2, meter.count).bindOrNull(3, meter.value)
                                .bindOrNull(4, meter.sum).bindOrNull(5, meter.mean)
                                .bindOrNull(6, meter.activeTasks).bindOrNull(7, meter.duration)
                                .bindOrNull(8, meter.max).bind(9, meter.name).bind(10, meter.tags)
                                .bindOrNull(11, meter.other)
                            val hasAnotherElement = (index + 1) - timescaledbMeters.size != 0
                            if (hasAnotherElement) statement.add()
                        }
                    }

                    Mono.from(statement.execute())
                        .map { it.rowsUpdated }
                        .doOnTerminate { Mono.from(it.close()).subscribe() }
                }.awaitLast().awaitLast()
            }
            log.debug { "Successfully sent ${meters.size} meters to Timescaledb" }
        } catch (e: Throwable) {
            log.error(e) { "Failed to send metrics to Timescaledb" }
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
            return timescaledbMeter.copy(count = value)
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for Gauge.
     */
    private fun convertGauge(gauge: Gauge, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val value = gauge.value()
        if (java.lang.Double.isFinite(value)) {
            return timescaledbMeter.copy(value = value)
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for TimeGauge.
     */
    private fun convertTimeGauge(gauge: TimeGauge, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val value = gauge.value(baseTimeUnit)
        if (java.lang.Double.isFinite(value)) {
            return timescaledbMeter.copy(value = value)
        }
        return timescaledbMeter
    }

    /**
     * Timescaledb converter for FunctionTimer.
     */
    private fun convertFunctionTimer(timer: FunctionTimer, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val sum = timer.totalTime(baseTimeUnit)
        val mean = timer.mean(TimeUnit.MILLISECONDS)
        return timescaledbMeter.copy(count = timer.count(), sum = sum, mean = mean)
    }

    /**
     * Timescaledb converter for LongTaskTimer.
     */
    private fun convertLongTaskTimer(timer: LongTaskTimer, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        return timescaledbMeter.copy(activeTasks = timer.activeTasks(), duration = timer.duration(baseTimeUnit))
    }

    /**
     * Timescaledb converter for Timer.
     */
    private fun convertTimer(timer: Timer, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        return timescaledbMeter.copy(
            count = timer.count().toDouble(),
            sum = timer.totalTime(baseTimeUnit),
            mean = timer.mean(baseTimeUnit),
            max = timer.max(baseTimeUnit)
        )
    }

    /**
     * Timescaledb serializer for DistributionSummary.
     */
    private fun convertSummary(summary: DistributionSummary, timescaledbMeter: TimescaledbMeter): TimescaledbMeter {
        val histogramSnapshot = summary.takeSnapshot()
        return timescaledbMeter.copy(
            count = histogramSnapshot.count().toDouble(),
            sum = histogramSnapshot.total(),
            mean = histogramSnapshot.mean(),
            max = histogramSnapshot.max()
        )
    }

    /**
     * Timescaledb further converter for previous kinds of Meter
     */
    private fun convertMeter(meter: Meter, timescaledbMeter: TimescaledbMeter): TimescaledbMeter? {
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

    private fun generateTimestamp(): Instant {
        return Instant.ofEpochMilli(clock.wallTime())
    }

    private fun getTags(meter: Meter): MutableList<Tag> {
        return getConventionTags(meter.id)
    }

    private fun getName(meter: Meter): String {
        return getConventionName(meter.id)
    }

    private fun Statement.bindOrNull(index: Int, value: Double?): Statement {
        if (value == null) return bindOrNull(0.0, index, Double::class.java)
        return bindOrNull(value, index, Double::class.java)
    }

    private fun Statement.bindOrNull(index: Int, value: Int?): Statement {
        return bindOrNull(value, index, Integer::class.java)
    }

    private fun Statement.bindOrNull(index: Int, value: String?): Statement {
        return bindOrNull(value, index, String::class.java)
    }

    private fun <T> Statement.bindOrNull(value: Any?, index: Int, type: Class<T>): Statement {
        return if (value != null) {
            this.bind(index, value)
        } else {
            this.bindNull(index, type)
        }
    }

    private companion object {

        val log = logger()
    }
}