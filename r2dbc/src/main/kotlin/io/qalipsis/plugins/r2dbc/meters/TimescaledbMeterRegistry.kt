package io.qalipsis.plugins.r2dbc.meters

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.step.StepMeterRegistry
import io.micrometer.core.instrument.step.StepRegistryConfig
import java.util.concurrent.TimeUnit

class TimescaledbMeterRegistry(config: StepRegistryConfig, clock: Clock): StepMeterRegistry(config, clock) {
    override fun getBaseTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun publish() {
        TODO("Not yet implemented")
    }
}