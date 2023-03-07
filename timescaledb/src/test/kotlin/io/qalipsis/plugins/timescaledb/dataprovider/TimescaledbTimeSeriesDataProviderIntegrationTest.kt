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

package io.qalipsis.plugins.timescaledb.dataprovider

import io.qalipsis.plugins.timescaledb.TimescaleDbContainerProvider
import java.time.Duration
import kotlin.math.pow
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName

/**
 * @author Joël Valère
 */

@DisabledIfSystemProperty(
    named = "os.arch",
    matches = "aarch.*",
    disabledReason = "The required functions only run in TimescaleDB HA, not supported on ARM"
)
internal class TimescaledbTimeSeriesDataProviderIntegrationTest :
    AbstractTimescaledbTimeSeriesDataProviderIntegrationTest() {

    override val dbPort: Int
        get() = db.firstMappedPort

    companion object {

        @Container
        @JvmStatic
        val db: JdbcDatabaseContainer<*> = TimescaleDbContainerProvider()
            // The image timescaledb-ha is required for the hyper functions but is not compliant with ARM architectures.
            .newInstance(DockerImageName.parse("timescale/timescaledb-ha").withTag("pg14-latest"))
            .apply {
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(128 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
                waitingFor(Wait.forListeningPort())
                withStartupTimeout(Duration.ofSeconds(240))

                withDatabaseName(DB_NAME)
                withUsername(USERNAME)
                withPassword(PASSWORD)
                withCommand("postgres -c shared_preload_libraries=timescaledb -c log_error_verbosity=VERBOSE -c timescaledb.telemetry_level=OFF -c max_connections=100")
                withInitScript("pgsql-init-timescaledb.sql")
            }
    }
}