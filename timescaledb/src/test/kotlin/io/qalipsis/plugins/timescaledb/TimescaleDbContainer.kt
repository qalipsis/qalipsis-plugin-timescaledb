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

package io.qalipsis.plugins.timescaledb

import com.github.dockerjava.api.command.InspectContainerResponse
import io.qalipsis.api.logging.LoggerHelper.logger
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

internal class TimescaleDbContainer<SELF : TimescaleDbContainer<SELF>>(
    dockerImageName: DockerImageName
) : PostgreSQLContainer<SELF>(dockerImageName) {

    override fun getJdbcUrl(): String {
        val additionalUrlParams = constructUrlParameters("?", "&")
        return ("jdbc:postgresql://" + host + ":" + getMappedPort(POSTGRESQL_PORT)
                + "/" + databaseName + additionalUrlParams)
    }

    override fun containerIsStarting(containerInfo: InspectContainerResponse?) {
        super.containerIsStarting(containerInfo)
        followOutput(Slf4jLogConsumer(log).withSeparateOutputStreams())
    }

    private companion object {
        val log = logger()
    }
}