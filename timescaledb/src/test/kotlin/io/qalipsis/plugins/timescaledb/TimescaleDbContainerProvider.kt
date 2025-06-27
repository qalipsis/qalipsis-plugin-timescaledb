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

import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.JdbcDatabaseContainerProvider
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.jdbc.ConnectionUrl
import org.testcontainers.utility.DockerImageName

internal class TimescaleDbContainerProvider : JdbcDatabaseContainerProvider() {

    override fun supports(databaseType: String): Boolean {
        return databaseType == "timescaledb"
    }

    override fun newInstance(): JdbcDatabaseContainer<*> {
        return newInstance(DEFAULT_TAG)
    }

    override fun newInstance(tag: String): JdbcDatabaseContainer<*> {
        val dockerImageName = DockerImageName.parse(IMAGE).withTag(tag)
            .asCompatibleSubstituteFor(PostgreSQLContainer.IMAGE)
        return TimescaleDbContainer(dockerImageName)
    }

    fun newInstance(imageName: DockerImageName): JdbcDatabaseContainer<*> {
        return TimescaleDbContainer(imageName.asCompatibleSubstituteFor(PostgreSQLContainer.IMAGE))
    }

    override fun newInstance(connectionUrl: ConnectionUrl?): JdbcDatabaseContainer<*> {
        return newInstanceFromConnectionUrl(connectionUrl, USER_PARAM, PASSWORD_PARAM)
    }

    companion object {

        val USER_PARAM = "user"

        val PASSWORD_PARAM = "password"

        val IMAGE = "timescale/timescaledb"

        val DEFAULT_TAG = "latest-pg16"

    }
}
