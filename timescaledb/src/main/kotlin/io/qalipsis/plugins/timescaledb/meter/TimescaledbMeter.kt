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

import java.math.BigDecimal
import java.sql.Timestamp

/**
 * Representation of meter to save into TimescaleDB.
 *
 * @author Palina Bril
 */
internal data class TimescaledbMeter(
    val name: String,
    val tags: String?,
    val timestamp: Timestamp,
    val type: String,
    val tenant: String? = null,
    val campaign: String? = null,
    val scenario: String? = null,
    val count: BigDecimal? = null,
    val value: BigDecimal? = null,
    val sum: BigDecimal? = null,
    val mean: BigDecimal? = null,
    val activeTasks: Int? = null,
    val duration: BigDecimal? = null,
    val unit: String? = null,
    val max: BigDecimal? = null,
    val other: String? = null
)