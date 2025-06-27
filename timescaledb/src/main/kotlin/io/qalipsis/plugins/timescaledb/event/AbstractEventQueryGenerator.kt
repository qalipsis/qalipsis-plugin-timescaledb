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