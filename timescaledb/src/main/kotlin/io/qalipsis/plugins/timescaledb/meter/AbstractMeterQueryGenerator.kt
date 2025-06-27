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

package io.qalipsis.plugins.timescaledb.meter

import io.qalipsis.api.report.DataField
import io.qalipsis.api.report.DataFieldType
import io.qalipsis.plugins.timescaledb.dataprovider.AbstractQueryGenerator
import io.qalipsis.plugins.timescaledb.dataprovider.DataType
import java.util.concurrent.TimeUnit

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
            DataField("duration_nano", DataFieldType.NUMBER, TimeUnit.NANOSECONDS.toString()),
            DataField("max", DataFieldType.NUMBER),
            DataField("other", DataFieldType.OBJECT)
        ).sortedBy { it.name }

    }

}