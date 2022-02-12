package io.qalipsis.plugins.r2dbc.events

import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.events.AbstractBufferedEventsPublisher
import io.qalipsis.api.events.Event
import io.qalipsis.api.events.EventJsonConverter
import io.qalipsis.plugins.r2dbc.config.TimescaledbEventsConfiguration
import io.r2dbc.pool.ConnectionPool
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import reactor.core.publisher.Mono
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

    override suspend fun publish(values: List<Event>) {
        databaseClient.create().flatMap {
            Mono.from(it.createStatement("").execute())
        }
    }


}
