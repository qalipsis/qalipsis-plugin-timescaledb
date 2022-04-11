package io.qalipsis.plugins.r2dbc.events

import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.events.AbstractBufferedEventsPublisher
import io.qalipsis.api.events.Event
import io.qalipsis.api.lang.durationSinceNanos
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.r2dbc.config.TimescaledbEventsConfiguration
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.pool.ConnectionPoolConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.Statement
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitLast
import kotlinx.coroutines.runBlocking
import liquibase.pro.packaged.T
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZonedDateTime
import kotlin.coroutines.CoroutineContext

/**
 * Implementation of [io.qalipsis.api.events.EventsLogger] for TimescaleDB.
 *
 * @author Gabriel Moraes
 */
@Singleton
@Requires(beans = [TimescaledbEventsConfiguration::class])
internal class TimescaledbEventsPublisher(
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineScope: CoroutineScope,
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineContext: CoroutineContext,
    private val configuration: TimescaledbEventsConfiguration,
    private val meterRegistry: MeterRegistry,
    private val eventsConverter: TimescaledbEventConverter
) : AbstractBufferedEventsPublisher(
    configuration.minLevel,
    configuration.lingerPeriod,
    configuration.batchSize,
    coroutineScope
) {

    private lateinit var publicationLatch: SuspendedCountLatch

    private lateinit var databaseClient: ConnectionPool

    override fun start() {
        databaseClient = ConnectionPool(
            ConnectionPoolConfiguration.builder()
                .connectionFactory(
                    PostgresqlConnectionFactory(
                        PostgresqlConnectionConfiguration.builder().host(configuration.host)
                            .username(configuration.username)
                            .password(configuration.password)
                            .database(configuration.database)
                            .schema(configuration.schema)
                            .port(configuration.port)
                            .build()
                    )
                ).build()
        )
        publicationLatch = SuspendedCountLatch(0)
        super.start()
    }

    override suspend fun publish(values: List<Event>) {
        publicationLatch.increment()
        coroutineScope.launch {
            try {
                performPublish(values)
            } finally {
                publicationLatch.decrement()
            }
        }

    }

    private suspend fun performPublish(values: List<Event>) {
        log.debug { "Sending ${values.size} events to Timescaledb" }
        val conversionStart = System.nanoTime()
        val timescaledbEvents = values.map { eventsConverter.convert(it) }

        meterRegistry.timer(EVENTS_CONVERSIONS_TIMER_NAME, "publisher", "timescaledb")
            .record(conversionStart.durationSinceNanos())
        val numberOfSentConverted = timescaledbEvents.size
        meterRegistry.counter(EVENTS_COUNT_TIMER_NAME, "publisher", "timescaledb")
            .increment(numberOfSentConverted.toDouble())

        val exportStart = System.nanoTime()
        try {

            databaseClient.create().flatMap {
                val statement = it.createStatement(
                    """insert into events (timestamp, level, tags, name, message, error, stack_trace, date, boolean, number, value) 
                    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"""
                )

                timescaledbEvents.mapIndexed { index, event ->
                    statement.bind(0, event.timestamp)
                        .bind(1, event.level).bind(2, event.tags)
                        .bind(3, event.name).bindOrNull(4, event.message)
                        .bindOrNull(5, event.error).bindOrNull(6, event.stackTrace)
                        .bindOrNull(7, event.date).bindOrNull(8, event.boolean)
                        .bindOrNull(9, event.number).bindOrNull(10, event.value)
                    val hasAnotherElement = (index + 1) - numberOfSentConverted != 0
                    if (hasAnotherElement) statement.add()
                }

                Mono.from(statement.execute())
                    .map { it.rowsUpdated }
                    .doOnTerminate { Mono.from(it.close()).subscribe() }
            }.awaitLast().awaitLast()

            val exportEnd = System.nanoTime()

            meterRegistry.timer(EVENTS_EXPORT_TIMER_NAME, "publisher", "timescaledb", "status", "success")
                .record(Duration.ofNanos(exportEnd - exportStart))

            log.debug { "onSuccess totally processed" }
        } catch (e: Exception) {
            log.debug { "failed to persist events" }
            meterRegistry.timer(EVENTS_EXPORT_TIMER_NAME, "publisher", "timescaledb", "status", "error")
                .record(exportStart.durationSinceNanos())
            log.error(e) { e.message }
        }

    }

    override fun stop() {
        log.debug { "Stopping the events logger with ${buffer.size} events in the buffer" }
        super.stop()
        runBlocking(coroutineContext) {
            log.debug { "Waiting for ${publicationLatch.get()} publication jobs to be completed" }
            publicationLatch.await()
        }
        tryAndLogOrNull(log) {
            databaseClient.close()
        }
        log.debug { "The events logger was stopped" }
    }

    companion object {


        private const val EVENTS_CONVERSIONS_TIMER_NAME = "timescaledb.events.conversion"

        private const val EVENTS_COUNT_TIMER_NAME = "timescaledb.events.converted"

        private const val EVENTS_EXPORT_TIMER_NAME = "timescaledb.events.export"

        @JvmStatic
        private val log = logger()
    }

    private fun Statement.bindOrNull(index: Int, value: LocalDateTime?): Statement {
        return bindOrNull(value, index, LocalDateTime::class.java)
    }

    private fun Statement.bindOrNull(index: Int, value: Instant?): Statement {
        return bindOrNull(value, index, Instant::class.java)
    }

    private fun Statement.bindOrNull(index: Int, value: ZonedDateTime?): Statement {
        return bindOrNull(value, index, ZonedDateTime::class.java)
    }

    private fun Statement.bindOrNull(index: Int, value: String?): Statement {
        return bindOrNull(value, index, String::class.java)
    }

    private fun Statement.bindOrNull(index: Int, value: BigDecimal?): Statement {
        return bindOrNull(value, index, BigDecimal::class.java)
    }

    private fun Statement.bindOrNull(index: Int, value: Boolean?): Statement {
        return bindOrNull(value, index, Boolean::class.java)
    }

    private fun <T> Statement.bindOrNull(value: Any?, index: Int, type: Class<T>): Statement {
        return if (value != null) {
            this.bind(index, value)
        } else {
            this.bindNull(index, type)
        }
    }
}
