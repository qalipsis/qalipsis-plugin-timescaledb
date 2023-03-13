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

import io.qalipsis.api.logging.LoggerHelper.logger
import io.r2dbc.spi.Statement
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

internal abstract class AbstractQueryExecutor<T> {

    abstract suspend fun execute(): T

    /**
     * Adds the additional clauses for the selected campaigns and scenarios.
     */
    protected fun buildAdditionalClauses(
        campaignsReferences: Set<String>,
        scenariosNames: Set<String>,
        actualBoundParameters: MutableMap<String, BoundParameter>,
        nextParameterIndex: Int,
        dataType: DataType? = null
    ): StringBuilder {
        var nextIdentifier = "$${nextParameterIndex}"
        actualBoundParameters[nextIdentifier] =
            RawBoundParameter(campaignsReferences.toTypedArray(), nextIdentifier)
        val additionalClauses = StringBuilder(" AND campaign = any (array[${nextIdentifier}])")

        if (scenariosNames.isNotEmpty()) {
            nextIdentifier = "$${nextParameterIndex + 1}"
            actualBoundParameters[nextIdentifier] =
                RawBoundParameter(scenariosNames.toTypedArray(), nextIdentifier)
            additionalClauses.append(" AND scenario = any (array[${nextIdentifier}])")
        } else if (dataType == DataType.METER) {
            additionalClauses.append(" AND scenario IS NULL")
        }
        return additionalClauses
    }

    /**
     * Binds the runtime arguments
     */
    protected fun bindArguments(
        tenant: String,
        statement: Statement,
        boundParameter: Map<String, BoundParameter>,
        actualStart: Instant,
        actualEnd: Instant
    ) {
        // Bind the hard-coded arguments.
        log.trace { "Binding $actualStart to ${boundParameter[":start"]!!.identifiers.first()}" }
        statement.bind(boundParameter[":start"]!!.identifiers.first(), actualStart)
        log.trace { "Binding $actualEnd to ${boundParameter[":end"]!!.identifiers.first()}" }
        statement.bind(boundParameter[":end"]!!.identifiers.first(), actualEnd)
        boundParameter[":tenant"]?.let {
            val actualTenant = it.value ?: tenant
            log.trace { "Binding $actualTenant to ${it.identifiers.first()}" }
            statement.bind(it.identifiers.first(), actualTenant)
        }

        // Bind the non-hard-coded arguments.
        boundParameter.filter { !it.key.startsWith(":") && it.value.identifiers.isNotEmpty() }.values.forEach { param ->
            param.identifiers.forEach { identifier ->
                if (param.value != null) {
                    log.trace {
                        "Binding ${
                            if (param.value is Array<*>) {
                                (param.value as Array<*>).joinToString()
                            } else {
                                "$param.value"
                            }
                        } to $identifier"
                    }
                    statement.bind(identifier, param.value!!)
                } else {
                    log.trace { "Binding a null value of type ${param.valueType} to $identifier" }
                    statement.bindNull(identifier, param.valueType)
                }
            }
        }
    }

    protected fun roundStartAndEnd(actualTimeframe: Long, start: Instant, end: Instant): Pair<Instant, Instant> {
        return when {
            actualTimeframe <= Duration.ofSeconds(10)
                .toMillis() -> start.truncatedTo(ChronoUnit.SECONDS) to (end.truncatedTo(ChronoUnit.SECONDS) + Duration.ofSeconds(
                1
            ))
            actualTimeframe <= Duration.ofMinutes(10)
                .toMillis() -> start.truncatedTo(ChronoUnit.MINUTES) to (end.truncatedTo(ChronoUnit.MINUTES) + Duration.ofMinutes(
                1
            ))
            else -> start.truncatedTo(ChronoUnit.HOURS) to (end.truncatedTo(ChronoUnit.HOURS) + Duration.ofHours(1))
        }
    }

    private companion object {
        val log = logger()
    }
}