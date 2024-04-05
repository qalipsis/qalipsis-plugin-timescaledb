/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.timescaledb.meter

import io.micrometer.core.instrument.config.MeterRegistryConfigValidator
import io.micrometer.core.instrument.config.validate.PropertyValidator
import io.micrometer.core.instrument.config.validate.Validated
import io.micrometer.core.instrument.step.StepRegistryConfig
import io.r2dbc.postgresql.client.SSLMode


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

    fun enableSsl(): Boolean {
        return PropertyValidator.getBoolean(this, "enableSsl").orElse(false)
    }

    fun sslMode(): SSLMode {
        return PropertyValidator.getEnum(this, SSLMode::class.java, "sslMode").orElse(SSLMode.PREFER)
    }

    fun sslRootCert(): String? {
        return PropertyValidator.getString(this, "sslRootCert").orElse(null)
    }

    fun sslKey(): String? {
        return PropertyValidator.getString(this, "sslKey").orElse(null)
    }

    fun sslCert(): String? {
        return PropertyValidator.getString(this, "sslCert").orElse(null)
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
     * Specifies whether the schema for the meters should be created or updated at startup.
     */
    fun initSchema(): Boolean {
        return PropertyValidator.getBoolean(this, "init-schema").orElse(true)
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
