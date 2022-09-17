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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import io.micronaut.core.annotation.Introspected
import java.math.BigDecimal

/**
 * Prepared query to retrieve and aggregate time-series data.
 *
 * @property dataType the type of records to apply the queries to
 * @property aggregationStatement SQL statement to apply for the aggregation calculation
 * @property aggregationBoundParameters parameters to bound to [aggregationStatement], keyed by an internal key relevant for later retrieval
 * @property nextAvailableAggregationParameterIdentifierIndex the next available SQL parameter binding index for [aggregationStatement], initialized to 1
 * @property countStatement SQL statement to count records matching the criteria
 * @property retrievalStatement SQL statement to retrieve records matching the criteria
 * @property retrievalBoundParameters parameters to bound to [countStatement] and [retrievalStatement], keyed by an internal key relevant for later retrieval
 * @property nextAvailableRetrievalParameterIdentifierIndex the next available SQL parameter binding index for [aggregationStatement], initialized to 1
 *
 * @author Eric Jessé
 */
@Introspected
internal class PreparedQueries(
    @JsonProperty("ty")
    val dataType: DataType
) {

    @JsonProperty("agSql")
    lateinit var aggregationStatement: String

    @JsonProperty("agPar")
    val aggregationBoundParameters = mutableMapOf<String, SerializableBoundParameter>()

    @JsonProperty("agIdx")
    var nextAvailableAggregationParameterIdentifierIndex: Int = 1

    @JsonProperty("ctSql")
    lateinit var countStatement: String

    @JsonProperty("rtSql")
    lateinit var retrievalStatement: String

    @JsonProperty("rtPar")
    val retrievalBoundParameters = mutableMapOf<String, SerializableBoundParameter>()

    @JsonProperty("rtIdx")
    var nextAvailableRetrievalParameterIdentifierIndex: Int = 1

    /**
     * Adds a parameter bound to the aggregation SQL statement.
     */
    fun bindAggregationParameter(key: String, parameter: SerializableBoundParameter) {
        aggregationBoundParameters[key] = parameter
        nextAvailableAggregationParameterIdentifierIndex =
            getNextUserIdentifierIndex(parameter.identifiers)
                .coerceAtLeast(nextAvailableAggregationParameterIdentifierIndex)
    }

    /**
     * Adds a parameter bound to the count and retrieval SQL statement.
     */
    fun bindCountAndRetrievalParameter(key: String, parameter: SerializableBoundParameter) {
        retrievalBoundParameters[key] = parameter
        nextAvailableRetrievalParameterIdentifierIndex =
            getNextUserIdentifierIndex(parameter.identifiers)
                .coerceAtLeast(nextAvailableRetrievalParameterIdentifierIndex)
    }

    /**
     * Determines the SQL identifier that can be used to bind the next parameter.
     */
    private fun getNextUserIdentifierIndex(identifiers: Collection<String>): Int {
        return identifiers.maxOf { it.substringAfter('$').toIntOrNull() ?: 0 } + 1
    }

    override fun toString(): String {
        return "PreparedQueries(dataType=$dataType, aggregationStatement='$aggregationStatement', aggregationBoundParameters=$aggregationBoundParameters, nextAvailableAggregationParameterIdentifierIndex=$nextAvailableAggregationParameterIdentifierIndex, countStatement='$countStatement', retrievalStatement='$retrievalStatement', retrievalBoundParameters=$retrievalBoundParameters, nextAvailableRetrievalParameterIdentifierIndex=$nextAvailableRetrievalParameterIdentifierIndex)"
    }

}

/**
 * Type of the data to read, to properly decide the connection to use.
 *
 * @author Eric Jessé
 */
@Introspected
enum class DataType {
    EVENT, METER
}

internal interface BoundParameter {
    val value: Any?
    val valueType: Class<*>
    val identifiers: List<String>
}

/**
 * Simple [BoundParameter] for immediate use.
 *
 * @property value value to bind
 * @property identifiers list of all the placeholders for the value in the statement
 */
internal data class RawBoundParameter(
    override val value: Any,
    override val valueType: Class<*> = value.javaClass,
    override val identifiers: List<String>
) : BoundParameter {

    constructor(
        value: Any,
        vararg identifiers: String
    ) : this(value, identifiers = identifiers.toList())

    override fun toString(): String {
        val serializedValue = if (value is Array<*>) {
            value.joinToString()
        } else {
            "$value"
        }
        return "RawBoundParameter(value=$serializedValue, valueType=$valueType, identifiers=$identifiers)"
    }


}

/**
 * Argument to bind to the statement, that can be serialized.
 *
 * @property serializedValue value as a string
 * @property type type of the concrete value
 * @property identifiers list of all the placeholders for the value in the statement
 */
@Introspected
internal data class SerializableBoundParameter(
    @JsonProperty("v")
    val serializedValue: String? = null,
    @JsonProperty("t")
    val type: Type,
    @JsonProperty("i")
    override val identifiers: List<String>
) : BoundParameter {

    constructor(
        serializedValue: String? = null,
        type: Type,
        vararg identifiers: String
    ) : this(serializedValue, type, identifiers.toList())

    @get:JsonIgnore
    override val value: Any? = serializedValue?.run {
        when (type) {
            Type.BOOLEAN -> trim().toBoolean()
            Type.NUMBER -> trim().toBigDecimal()
            Type.NUMBER_ARRAY -> split(",").map { it.trim().toBigDecimal() }.toTypedArray()
            Type.STRING -> trim()
            Type.STRING_ARRAY -> split(",").map { it.trim() }.toTypedArray()
        }
    }

    @get:JsonIgnore
    override val valueType: Class<*> = when (type) {
        Type.BOOLEAN -> Boolean::class.java
        Type.NUMBER -> BigDecimal::class.java
        Type.NUMBER_ARRAY -> BIG_DECIMAL_ARRAY
        Type.STRING -> String::class.java
        Type.STRING_ARRAY -> STRING_ARRAY
    }

    enum class Type(private val rawType: Type?, val isArray: Boolean = false) {
        STRING(null),
        STRING_ARRAY(STRING, true),
        NUMBER(null),
        NUMBER_ARRAY(NUMBER, true),
        BOOLEAN(null);

        val raw: Type
            get() = rawType ?: this
    }

    private companion object {
        val BIG_DECIMAL_ARRAY = emptyArray<BigDecimal>().javaClass
        val STRING_ARRAY = emptyArray<String>().javaClass
    }
}