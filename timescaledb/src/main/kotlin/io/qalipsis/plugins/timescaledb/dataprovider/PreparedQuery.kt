package io.qalipsis.plugins.timescaledb.dataprovider

import io.micronaut.core.annotation.Introspected

/**
 * Prepared query to retrieve data.
 */
@Introspected
internal data class PreparedQuery(
    val aggregationStatement: String,
    val aggregationBoundParameters: Map<String, BoundParameters>,
    val retrievalStatement: String,
    val retrievalBoundParameters: Map<String, BoundParameters>
)

/**
 * Argument to bind to the statement.
 *
 * @property value value
 * @property identifiers list of all the placeholders for the value in the statement
 */
@Introspected
internal data class BoundParameters(
    val value: String? = null,
    val type: Type,
    val identifiers: List<String>
) {

    constructor(
        value: String? = null,
        type: Type,
        vararg identifiers: String
    ) : this(value, type, identifiers.toList())

    enum class Type(private val rawType: Type?, val isArray: Boolean = false) {
        STRING(null),
        STRING_ARRAY(STRING, true),
        NUMBER(null),
        NUMBER_ARRAY(NUMBER, true),
        BOOLEAN(null);

        val raw: Type
            get() = rawType ?: this
    }
}