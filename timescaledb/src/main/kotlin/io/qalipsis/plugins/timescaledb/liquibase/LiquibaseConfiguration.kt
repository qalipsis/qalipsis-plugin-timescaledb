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

package io.qalipsis.plugins.timescaledb.liquibase

import io.r2dbc.postgresql.client.SSLMode

internal data class LiquibaseConfiguration(
    val changeLog: String,
    val host: String,
    val port: Int,
    val username: String,
    val password: String,
    val database: String,
    val enableSsl: Boolean,
    val sslMode: SSLMode,
    val sslRootCert: String?,
    val sslCert: String?,
    val sslKey: String?,
    val defaultSchemaName: String,
    val liquibaseSchemaName: String = defaultSchemaName
)
