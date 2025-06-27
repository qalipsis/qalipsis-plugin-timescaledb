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

import java.math.BigDecimal
import java.sql.Timestamp

/**
 * Representation of event to save into TimescaleDB.
 *
 * @author Gabriel Moraes
 */
internal data class TimescaledbEvent(
    val id: Long? = null,
    val timestamp: Timestamp,
    val level: String,
    val name: String,
    val tenant: String? = null,
    val campaign: String? = null,
    val scenario: String? = null,
    val tags: String? = null,
    val message: String? = null,
    val stackTrace: String? = null,
    val error: String? = null,
    val date: Timestamp? = null,
    val boolean: Boolean? = null,
    val number: BigDecimal? = null,
    val durationNano: BigDecimal? = null,
    val geoPoint: String? = null,
    val value: String? = null
)
