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
import io.qalipsis.api.query.QueryAggregationOperator
import io.qalipsis.api.query.QueryClause
import io.qalipsis.api.query.QueryClauseOperator
import io.qalipsis.api.query.QueryDescription
import io.qalipsis.api.report.DataField

internal abstract class AbstractQueryGenerator(
    private val dataType: DataType,
    private val databaseTable: String,
    val queryFields: List<DataField>,
    private val numericFields: Set<String>,
    private val booleanFields: Set<String>,
    private val jsonFields: Set<String>
) {

    private val queryFieldsByName = queryFields.associateBy { it.name }

    /**
     * Prepares the queries to fetch both aggregation and data according to the query description.
     */
    fun prepareQueries(tenant: String, query: QueryDescription): PreparedQueries {
        log.debug { "Creating queries for the tenant $tenant and $query" }
        val prepareQueries = PreparedQueries(dataType)
        prepareAggregationQuery(tenant, query, prepareQueries)
        prepareCountAndRetrievalQuery(tenant, query, prepareQueries)
        log.debug { "Created queries: $prepareQueries" }
        return prepareQueries
    }

    /**
     * Prepares the query to aggregate the data in time-buckets.
     */
    private fun prepareAggregationQuery(tenant: String, query: QueryDescription, preparedQueries: PreparedQueries) {
        if (query.aggregationOperation != QueryAggregationOperator.COUNT) {
            require(query.fieldName in numericFields) { "The field ${query.fieldName} is not numeric and cannot be aggregated" }
        }
        addDefaultParametersForAggregationStatement(tenant, query.timeframeUnit?.toMillis(), preparedQueries)

        val sql = buildRootQueryForAggregation(query, preparedQueries.aggregationBoundParameters)
        sql.append(" WHERE ${databaseTable}.timestamp BETWEEN $1::timestamp AND $2::timestamp AND ${databaseTable}.tenant = $3")

        if (query.fieldName != null) {
            // If count aggregation and field name are set, select only the records where the field is not null.
            sql.append(""" AND ${databaseTable}.${query.fieldName} IS NOT NULL""")
        }
        addClauses(
            queryClauses = query.filters,
            sql = sql,
            boundParametersCollector = preparedQueries::bindAggregationParameter,
            nextIdentifierIndexSupplier = preparedQueries::nextAvailableAggregationParameterIdentifierIndex
        )

        appendGroupingForAggregation(sql)
        appendOrderingForAggregation(sql)

        preparedQueries.aggregationStatement = sql.toString()
    }

    private fun addDefaultParametersForAggregationStatement(
        tenant: String,
        timeframeMillis: Long?,
        preparedQueries: PreparedQueries
    ) {
        // The interval value cannot be bound and must be replaced as a string.
        preparedQueries.bindAggregationParameter(
            ":timeframe", SerializableBoundParameter(
                serializedValue = "${timeframeMillis ?: 10_000}",
                SerializableBoundParameter.Type.NUMBER,
                "%timeframe%"
            )
        )
        preparedQueries.bindAggregationParameter(
            ":start",
            SerializableBoundParameter(serializedValue = null, SerializableBoundParameter.Type.STRING, "$1")
        )
        preparedQueries.bindAggregationParameter(
            ":end",
            SerializableBoundParameter(serializedValue = null, SerializableBoundParameter.Type.STRING, "$2")
        )
        preparedQueries.bindAggregationParameter(
            "tenant",
            SerializableBoundParameter(serializedValue = tenant, SerializableBoundParameter.Type.STRING, "$3")
        )
    }

    abstract fun buildRootQueryForAggregation(
        query: QueryDescription,
        boundParameters: Map<String, SerializableBoundParameter>
    ): StringBuilder

    abstract fun appendGroupingForAggregation(sql: StringBuilder)

    abstract fun appendOrderingForAggregation(sql: StringBuilder)

    /**
     * Prepares the statements to count and to retrieve the data in time-buckets.
     */
    private fun prepareCountAndRetrievalQuery(
        tenant: String,
        query: QueryDescription,
        preparedQueries: PreparedQueries
    ) {
        addDefaultParametersForCountAndRetrievalStatement(tenant, preparedQueries)
        val sql = StringBuilder("")
        sql.append(" FROM $databaseTable WHERE ${databaseTable}.timestamp BETWEEN $1::timestamp AND $2::timestamp AND ${databaseTable}.tenant = $3")

        if (query.fieldName != null) {
            // If count aggregation and field name are set, select only the records where the field is not null.
            sql.append(""" AND ${databaseTable}.${query.fieldName} IS NOT NULL""")
        }

        addClauses(
            queryClauses = query.filters,
            sql = sql,
            boundParametersCollector = preparedQueries::bindCountAndRetrievalParameter,
            nextIdentifierIndexSupplier = preparedQueries::nextAvailableRetrievalParameterIdentifierIndex
        )
        preparedQueries.countStatement = "SELECT COUNT(*) $sql"
        preparedQueries.retrievalStatement =
            "SELECT * $sql ORDER BY ${databaseTable}.timestamp %order% LIMIT %limit% OFFSET %offset%"
    }

    private fun addDefaultParametersForCountAndRetrievalStatement(tenant: String, preparedQueries: PreparedQueries) {
        preparedQueries.bindCountAndRetrievalParameter(
            ":limit",
            SerializableBoundParameter(serializedValue = "100", SerializableBoundParameter.Type.NUMBER, "%limit%")
        )
        preparedQueries.bindCountAndRetrievalParameter(
            ":offset",
            SerializableBoundParameter(serializedValue = "0", SerializableBoundParameter.Type.NUMBER, "%offset%")
        )
        preparedQueries.bindCountAndRetrievalParameter(
            ":order",
            SerializableBoundParameter(serializedValue = "DESC", SerializableBoundParameter.Type.STRING, "%order%")
        )
        preparedQueries.bindCountAndRetrievalParameter(
            ":start",
            SerializableBoundParameter(serializedValue = null, SerializableBoundParameter.Type.STRING, "$1")
        )
        preparedQueries.bindCountAndRetrievalParameter(
            ":end",
            SerializableBoundParameter(serializedValue = null, SerializableBoundParameter.Type.STRING, "$2")
        )
        preparedQueries.bindCountAndRetrievalParameter(
            "tenant",
            SerializableBoundParameter(serializedValue = tenant, SerializableBoundParameter.Type.STRING, "$3")
        )
    }

    private fun addClauses(
        queryClauses: Collection<QueryClause>,
        sql: StringBuilder,
        boundParametersCollector: (key: String, SerializableBoundParameter) -> Unit,
        nextIdentifierIndexSupplier: () -> Int
    ) {
        sql.append(" %s") // Placeholder for additional filters (specific campaigns or scenarios)
        queryClauses.forEach { clause ->
            if (clause.name == "name" || clause.name in queryFieldsByName.keys) {
                sql.append(""" AND ${databaseTable}.${clause.name}""")
                sql.append(
                    """ ${
                        convertComparator(
                            clause.name,
                            clause.operator,
                            clause.value,
                            boundParametersCollector,
                            nextIdentifierIndexSupplier()
                        )
                    }"""
                )
            } else {
                sql.append(""" AND ${databaseTable}.tags->>'${clause.name}'""")
                sql.append(
                    """ ${
                        convertComparator(
                            null,
                            clause.operator,
                            clause.value,
                            boundParametersCollector,
                            nextIdentifierIndexSupplier()
                        )
                    }"""
                )
            }
        }
    }

    private fun convertComparator(
        fieldName: String?,
        operator: QueryClauseOperator,
        value: String,
        boundParametersCollector: (key: String, SerializableBoundParameter) -> Unit,
        nextIdentifierIndex: Int
    ): String {
        val bindingParam = "$${nextIdentifierIndex}"
        var paramType = resolveParameterType(fieldName, value)

        val criteria = when (operator) {
            QueryClauseOperator.IS_IN -> "= any (array[$bindingParam])"
            QueryClauseOperator.IS_NOT_IN -> "<> all (array[$bindingParam])"
            QueryClauseOperator.IS_LIKE -> "ILIKE " + if (paramType.isArray) {
                "any (array[$bindingParam])"
            } else {
                bindingParam
            }
            QueryClauseOperator.IS_NOT_LIKE -> "NOT ILIKE " + if (paramType.isArray) {
                "all (array[$bindingParam])"
            } else {
                bindingParam
            }
            else -> {
                paramType = paramType.raw
                when (operator) {
                    QueryClauseOperator.IS -> "= $bindingParam"
                    QueryClauseOperator.IS_NOT -> "<> $bindingParam"
                    QueryClauseOperator.IS_GREATER_THAN -> "> $bindingParam"
                    QueryClauseOperator.IS_LOWER_THAN -> "< $bindingParam"
                    QueryClauseOperator.IS_GREATER_OR_EQUAL_TO -> ">= $bindingParam"
                    QueryClauseOperator.IS_LOWER_OR_EQUAL_TO -> "<= $bindingParam"
                    else -> throw UnsupportedOperationException("The operator $operator is not supported")
                }
            }
        }

        // Set the convenient parameter in the binding list.
        boundParametersCollector(bindingParam, SerializableBoundParameter(value, paramType, bindingParam))

        return criteria
    }

    /**
     * Resolves the type of the parameter to bind to the SQL query.
     *
     * @param fieldName name of the field in the database if not a key of tags
     * @param value the value to bind
     */
    private fun resolveParameterType(
        fieldName: String?,
        value: String
    ): SerializableBoundParameter.Type {
        return when (fieldName) {
            in booleanFields -> {
                SerializableBoundParameter.Type.BOOLEAN
            }
            in numericFields -> {
                if (value.contains(',')) {
                    SerializableBoundParameter.Type.NUMBER_ARRAY
                } else {
                    SerializableBoundParameter.Type.NUMBER
                }
            }
            else -> {
                if (value.contains(',')) {
                    SerializableBoundParameter.Type.STRING_ARRAY
                } else {
                    SerializableBoundParameter.Type.STRING
                }
            }
        }
    }

    private companion object {

        val log = logger()
    }
}