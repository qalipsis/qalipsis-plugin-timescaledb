package io.qalipsis.plugins.r2dbc.meters

import io.micrometer.core.instrument.config.MeterRegistryConfigValidator
import io.micrometer.core.instrument.config.validate.PropertyValidator
import io.micrometer.core.instrument.config.validate.Validated
import io.micrometer.core.instrument.step.StepRegistryConfig
import io.micrometer.core.lang.Nullable


/**
 * {@link MeterRegistry} for Timescaledb
 *
 * @author Palina Bril
 */
internal abstract class TimescaledbMeterConfig : StepRegistryConfig {

    override fun prefix(): String {
        return "timescaledb"
    }

    fun host(): String {
        return PropertyValidator.getString(this, "host").orElse("localhost")
    }

    fun port(): String {
        return PropertyValidator.getString(this, "port").orElse("5432")
    }

    fun database(): String {
        return PropertyValidator.getString(this, "database").orElse("qalipsis_db")
    }

    @Nullable
    fun userName(): String? {
        return PropertyValidator.getSecret(this, "username").orElse("qalipsis_user")
    }

    @Nullable
    fun password(): String? {
        return PropertyValidator.getSecret(this, "password").orElse("qalipsis-pwd")
    }

    fun schema(): String {
        return PropertyValidator.getString(this, "schema").orElse("qalipsis_ts")
    }

    /**
     * The name of the timestamp field. Default is: "timestamp"
     *
     * @return field name for timestamp
     */
    fun timestampFieldName(): String {
        return PropertyValidator.getString(this, "timestampFieldName").orElse("timestamp")
    }

    override fun validate(): Validated<*>? {
        return MeterRegistryConfigValidator.checkAll(this,
            { c: TimescaledbMeterConfig -> StepRegistryConfig.validate(c) },
            MeterRegistryConfigValidator.checkRequired("port") { obj: TimescaledbMeterConfig -> obj.port() },
            MeterRegistryConfigValidator.checkRequired("host") { obj: TimescaledbMeterConfig -> obj.host() },
            MeterRegistryConfigValidator.checkRequired("database") { obj: TimescaledbMeterConfig -> obj.database() },
            MeterRegistryConfigValidator.checkRequired("username") { obj: TimescaledbMeterConfig -> obj.userName() },
            MeterRegistryConfigValidator.checkRequired("password") { obj: TimescaledbMeterConfig -> obj.password() },
            MeterRegistryConfigValidator.checkRequired("schema") { obj: TimescaledbMeterConfig -> obj.schema() }
        )
    }
}
