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
