package io.qalipsis.plugins.timescaledb.meter

import io.micrometer.core.instrument.config.MeterRegistryConfigValidator
import io.micrometer.core.instrument.config.validate.PropertyValidator
import io.micrometer.core.instrument.config.validate.Validated
import io.micrometer.core.instrument.step.StepRegistryConfig


/**
 * {@link MeterRegistry} for Timescaledb
 *
 * @author Palina Bril
 */
abstract class TimescaledbMeterConfig : StepRegistryConfig {

    override fun prefix(): String {
        return "timescaledb"
    }

    fun host(): String {
        return PropertyValidator.getString(this, "host").orElse("localhost")
    }

    fun port(): Int {
        return PropertyValidator.getInteger(this, "port").orElse(5432)
    }

    fun database(): String {
        return PropertyValidator.getString(this, "database").orElse("qalipsis")
    }

    fun username(): String {
        return PropertyValidator.getSecret(this, "username").orElse("qalipsis_user")
    }

    fun password(): String {
        return PropertyValidator.getSecret(this, "password").orElse("qalipsis-pwd")
    }

    fun schema(): String {
        return PropertyValidator.getString(this, "schema").orElse("meters")
    }

    /**
     * For test purpose only.
     */
    fun autostart(): Boolean {
        return PropertyValidator.getBoolean(this, "autostart").orElse(true)
    }

    /**
     * For test purpose only.
     */
    fun autoconnect(): Boolean {
        return PropertyValidator.getBoolean(this, "autoconnect").orElse(true)
    }

    /**
     * The name of the timestamp field. Default is: "timestamp"
     *
     * @return field name for timestamp
     */
    fun timestampFieldName(): String {
        return "timestamp"
    }

    override fun validate(): Validated<*> {
        return MeterRegistryConfigValidator.checkAll(this,
            { c: TimescaledbMeterConfig -> StepRegistryConfig.validate(c) },
            MeterRegistryConfigValidator.checkRequired("host") { obj: TimescaledbMeterConfig -> obj.host() },
            MeterRegistryConfigValidator.checkRequired("port") { obj: TimescaledbMeterConfig -> obj.port() },
            MeterRegistryConfigValidator.checkRequired("username") { obj: TimescaledbMeterConfig -> obj.username() },
            MeterRegistryConfigValidator.checkRequired("password") { obj: TimescaledbMeterConfig -> obj.password() },
            MeterRegistryConfigValidator.checkRequired("schema") { obj: TimescaledbMeterConfig -> obj.schema() }
        )
    }
}
