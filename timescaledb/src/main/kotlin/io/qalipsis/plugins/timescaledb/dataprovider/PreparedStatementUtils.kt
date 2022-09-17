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
