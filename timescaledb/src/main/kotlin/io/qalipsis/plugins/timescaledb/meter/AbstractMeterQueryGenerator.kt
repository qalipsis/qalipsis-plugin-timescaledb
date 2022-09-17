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

package io.qalipsis.plugins.timescaledb.meter

import io.qalipsis.api.report.DataField
import io.qalipsis.api.report.DataFieldType
import io.qalipsis.plugins.timescaledb.dataprovider.AbstractQueryGenerator
import io.qalipsis.plugins.timescaledb.dataprovider.DataType

internal abstract class AbstractMeterQueryGenerator : AbstractQueryGenerator(
    dataType = DataType.METER,
    databaseTable = "meters",
    queryFields = FIELDS,
    numericFields = FIELDS.filter { it.type == DataFieldType.NUMBER }.map { it.name }.toSet(),
    booleanFields = FIELDS.filter { it.type == DataFieldType.BOOLEAN }.map { it.name }.toSet(),
    jsonFields = emptySet()
) {

    companion object {
        /**
         * List of fields that can be used for aggregation and filters.
         */
        val FIELDS = listOf(
            DataField("count", DataFieldType.NUMBER),
            DataField("value", DataFieldType.NUMBER),
            DataField("sum", DataFieldType.NUMBER),
            DataField("mean", DataFieldType.NUMBER),
            DataField("active_tasks", DataFieldType.NUMBER),
            DataField("duration", DataFieldType.NUMBER),
            DataField("max", DataFieldType.NUMBER),
            DataField("other", DataFieldType.OBJECT)
        ).sortedBy { it.name }

    }

}