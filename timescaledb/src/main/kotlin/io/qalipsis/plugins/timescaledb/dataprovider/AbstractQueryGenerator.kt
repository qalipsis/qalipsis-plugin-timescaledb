package io.qalipsis.plugins.timescaledb.dataprovider

import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.report.DataField
import io.qalipsis.api.report.query.QueryAggregationOperator
import io.qalipsis.api.report.query.QueryClauseOperator
import io.qalipsis.api.report.query.QueryDescription

internal abstract class AbstractQueryGenerator(
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
    fun prepareQueries(tenant: String, query: QueryDescription): PreparedQuery {
        log.debug { "Creating queries for the tenant $tenant and $query" }
        val (aggregationSql, aggregationBoundParameters) = prepareAggregationQuery(query, tenant)
        val (retrievalSql, retrievalBoundParameters) = prepareRetrievalQuery(query, tenant)

        return PreparedQuery(
            aggregationStatement = aggregationSql,
            aggregationBoundParameters = aggregationBoundParameters,
            retrievalStatement = retrievalSql,
            retrievalBoundParameters = retrievalBoundParameters
        ).also {
            log.debug { "Created queries: $it" }
        }
    }

    /**
     * Prepares the query to aggregate the data in time-buckets.
     */
    private fun prepareAggregationQuery(
        query: QueryDescription,
        tenant: String
    ): Pair<String, MutableMap<String, BoundParameters>> {
        if (query.aggregationOperation != QueryAggregationOperator.COUNT) {
            require(query.fieldName in numericFields) { "The field ${query.fieldName} is not numeric and cannot be aggregated" }
        }
        val boundParameters = mutableMapOf<String, BoundParameters>()
        // The interval value cannot be bound and must be replaced as a string.
        boundParameters[":timeframe"] =
            BoundParameters(
                value = "${query.timeframeUnit?.toMillis() ?: 10_000}",
                BoundParameters.Type.NUMBER,
                "%timeframe%"
            )
        boundParameters[":start"] = BoundParameters(value = null, BoundParameters.Type.STRING, "$1")
        boundParameters[":end"] = BoundParameters(value = null, BoundParameters.Type.STRING, "$2")
        boundParameters["tenant"] = BoundParameters(value = tenant, BoundParameters.Type.STRING, "$3")

        val sql = buildRootQueryForAggregation(query, boundParameters)
        sql.append(" WHERE ${databaseTable}.timestamp BETWEEN $1::timestamp AND $2::timestamp AND ${databaseTable}.tenant = $3")

        if (query.aggregationOperation == QueryAggregationOperator.COUNT && query.fieldName != null) {
            // If count aggregation and field name are set, select only the records where the field is not null.
            sql.append(""" AND ${databaseTable}.${query.fieldName} IS NOT NULL""")
        }
        sql.append(" %s") // Placeholder for additional filters (specific campaigns or scenarios)
        query.filters.forEach { clause ->
            if (clause.name == "name" || clause.name in queryFieldsByName.keys) {
                sql.append(""" AND ${databaseTable}.${clause.name}""")
                sql.append(
                    """ ${
                        convertComparator(
                            clause.name,
                            clause.operator,
                            clause.value,
                            boundParameters,
                            boundParameters.size
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
                            boundParameters,
                            boundParameters.size
                        )
                    }"""
                )
            }
        }

        appendGroupingForAggregation(sql)
        appendOrderingForAggregation(sql)
        return sql.toString() to boundParameters
    }

    abstract fun buildRootQueryForAggregation(
        query: QueryDescription,
        boundParameters: MutableMap<String, BoundParameters>
    ): StringBuilder

    abstract fun appendGroupingForAggregation(sql: StringBuilder)

    abstract fun appendOrderingForAggregation(sql: StringBuilder)

    /**
     * Prepares the query to retrieve the data in time-buckets.
     */
    private fun prepareRetrievalQuery(
        query: QueryDescription,
        tenant: String
    ): Pair<String, MutableMap<String, BoundParameters>> {
        val boundParameters = mutableMapOf<String, BoundParameters>()

        boundParameters[":limit"] = BoundParameters(value = "100", BoundParameters.Type.NUMBER, "%limit%")
        boundParameters[":order"] = BoundParameters(value = "DESC", BoundParameters.Type.STRING, "%order%")
        boundParameters[":start"] = BoundParameters(value = null, BoundParameters.Type.STRING, "$1")
        boundParameters[":end"] = BoundParameters(value = null, BoundParameters.Type.STRING, "$2")
        boundParameters["tenant"] = BoundParameters(value = tenant, BoundParameters.Type.STRING, "$3")

        val sql = StringBuilder("""SELECT * FROM ${databaseTable}""")
        sql.append(" WHERE ${databaseTable}.timestamp BETWEEN $1::timestamp AND $2::timestamp AND ${databaseTable}.tenant = $3")

        if (query.fieldName != null) {
            // If count aggregation and field name are set, select only the records where the field is not null.
            sql.append(""" AND ${databaseTable}.${query.fieldName} IS NOT NULL""")
        }

        sql.append(" %s") // Placeholder for additional filters (specific campaigns or scenarios)
        query.filters.forEach { clause ->
            if (clause.name == "name" || clause.name in queryFieldsByName.keys) {
                sql.append(""" AND ${databaseTable}.${clause.name}""")
                sql.append(
                    """ ${
                        convertComparator(
                            clause.name,
                            clause.operator,
                            clause.value,
                            boundParameters,
                            boundParameters.size - 1
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
                            boundParameters,
                            boundParameters.size - 1
                        )
                    }"""
                )
            }
        }

        sql.append(" ORDER BY ${databaseTable}.timestamp %order%")
        sql.append(" LIMIT %limit%")
        return sql.toString() to boundParameters
    }

    private fun convertComparator(
        fieldName: String?,
        operator: QueryClauseOperator,
        value: String,
        boundParameters: MutableMap<String, BoundParameters>,
        identifierIndex: Int
    ): String {
        val bindingParam = "$${identifierIndex}"
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
        boundParameters[bindingParam] = BoundParameters(value, paramType, bindingParam)

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
    ): BoundParameters.Type {
        return when (fieldName) {
            in booleanFields -> {
                BoundParameters.Type.BOOLEAN
            }
            in numericFields -> {
                if (value.contains(',')) {
                    BoundParameters.Type.NUMBER_ARRAY
                } else {
                    BoundParameters.Type.NUMBER
                }
            }
            else -> {
                if (value.contains(',')) {
                    BoundParameters.Type.STRING_ARRAY
                } else {
                    BoundParameters.Type.STRING
                }
            }
        }
    }

    private companion object {

        val log = logger()
    }
}