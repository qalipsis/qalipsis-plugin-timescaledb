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

package io.qalipsis.plugins.timescaledb.event

import io.qalipsis.api.report.DataField
import io.qalipsis.api.report.DataFieldType
import io.qalipsis.plugins.timescaledb.dataprovider.AbstractQueryGenerator
import io.qalipsis.plugins.timescaledb.dataprovider.DataType
import java.util.concurrent.TimeUnit

internal abstract class AbstractEventQueryGenerator : AbstractQueryGenerator(
    dataType = DataType.EVENT,
    databaseTable = "events",
    queryFields = FIELDS,
    numericFields = FIELDS.filter { it.type == DataFieldType.NUMBER }.map { it.name }.toSet(),
    booleanFields = FIELDS.filter { it.type == DataFieldType.BOOLEAN }.map { it.name }.toSet(),
    jsonFields = JSON_FIELDS,
) {

    companion object {
        /**
         * List of fields that can be used for aggregation and filters.
         */
        val FIELDS = listOf(
            DataField("message", DataFieldType.STRING),
            DataField("error", DataFieldType.STRING),
            DataField("stack_trace", DataFieldType.STRING),
            DataField("date", DataFieldType.DATE),
            DataField("boolean", DataFieldType.BOOLEAN),
            DataField("number", DataFieldType.NUMBER),
            DataField("duration_nano", DataFieldType.NUMBER, TimeUnit.NANOSECONDS.toString()),
            DataField("geo_point", DataFieldType.OBJECT),
            DataField("value", DataFieldType.OBJECT)
        ).sortedBy { it.name }

        /**
         * Fields having a value to compare as a JSON.
         */
        private val JSON_FIELDS = setOf("geo_point", "value")
    }

}