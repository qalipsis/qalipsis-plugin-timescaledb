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

import java.math.BigDecimal
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types


internal fun PreparedStatement.setStringOrNull(index: Int, value: String?) {
    if (value != null) {
        setString(index, value)
    } else {
        setNull(index, Types.VARCHAR)
    }
}

internal fun PreparedStatement.setTimestampOrNull(index: Int, value: Timestamp?) {
    if (value != null) {
        setTimestamp(index, value)
    } else {
        setNull(index, Types.TIMESTAMP_WITH_TIMEZONE)
    }
}

internal fun PreparedStatement.setBooleanOrNull(index: Int, value: Boolean?) {
    if (value != null) {
        setBoolean(index, value)
    } else {
        setNull(index, Types.BOOLEAN)
    }
}

internal fun PreparedStatement.setBigDecimalOrNull(index: Int, value: BigDecimal?) {
    if (value != null) {
        setBigDecimal(index, value)
    } else {
        setNull(index, Types.DOUBLE)
    }
}


internal fun PreparedStatement.setIntOrNull(index: Int, value: Int?) {
    if (value != null) {
        setInt(index, value)
    } else {
        setNull(index, Types.INTEGER)
    }
}
