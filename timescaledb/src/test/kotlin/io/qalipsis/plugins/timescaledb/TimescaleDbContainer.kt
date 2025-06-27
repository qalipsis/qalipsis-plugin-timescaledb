/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
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