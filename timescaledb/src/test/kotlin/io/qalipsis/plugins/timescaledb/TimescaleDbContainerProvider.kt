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

        val DEFAULT_TAG = "latest-pg14"

    }
}
