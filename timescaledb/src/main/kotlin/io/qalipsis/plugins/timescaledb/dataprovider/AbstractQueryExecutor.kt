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