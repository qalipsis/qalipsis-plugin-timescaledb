package io.qalipsis.plugins.r2dbc.events

import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.events.AbstractBufferedEventsPublisher
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventJsonConverter
import io.qalipsis.api.lang.durationSinceNanos
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.r2dbc.config.TimescaledbEventsConfiguration
import io.r2dbc.pool.ConnectionPool
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope.coroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.regex.Pattern
import kotlin.coroutines.CoroutineContext

/**
 *
 */
@Singleton
@Requires(beans = [TimescaledbEventsConfiguration::class])
internal class TimescaledbEventsPublisher(
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineScope: CoroutineScope,
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineContext: CoroutineContext,
    private val configuration: TimescaledbEventsConfiguration,
    private val meterRegistry: MeterRegistry,
    private val eventsConverter: EventJsonConverter,
    private val databaseClient: ConnectionPool
) : AbstractBufferedEventsPublisher(
    configuration.minLevel,
    configuration.lingerPeriod,
    configuration.batchSize,
    coroutineScope
) {

    private lateinit var publicationLatch: SuspendedCountLatch

    override fun start() {
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

        meterRegistry.timer(EVENTS_CONVERSIONS_TIMER_NAME, "publisher", "timescaledb")
            .record(conversionStart.durationSinceNanos())
        val numberOfSentItems = values.size
        meterRegistry.counter(EVENTS_COUNT_TIMER_NAME, "publisher", "timescaledb")
            .increment(numberOfSentItems.toDouble())
        val exportStart = System.nanoTime()

        try {
            databaseClient.create().flatMap {
                val statement = it.createStatement("insert into events () values ($1, $2)")
                    .bind(0, "").bind(1, "").add()
                Mono.from(statement.execute())
                    .timeout(Duration.ofSeconds(1))
                    .doOnTerminate { Mono.from(it.close()).subscribe() }
            }.awaitFirstOrNull()

            val exportEnd = System.nanoTime()

            meterRegistry.timer(EVENTS_EXPORT_TIMER_NAME, "publisher", "timescaledb", "status", "success")
                .record(Duration.ofNanos(exportEnd - exportStart))

            log.trace { "onSuccess totally processed" }
        } catch (e: Exception) {
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
        log.debug { "The events logger was stopped" }
    }

    companion object {


        private const val EVENTS_CONVERSIONS_TIMER_NAME = "timescaledb.events.conversion"

        private const val EVENTS_COUNT_TIMER_NAME = "timescaledb.events.converted"

        private const val EVENTS_EXPORT_TIMER_NAME = "timescaledb.events.export"

        @JvmStatic
        private val log = logger()
    }

}
