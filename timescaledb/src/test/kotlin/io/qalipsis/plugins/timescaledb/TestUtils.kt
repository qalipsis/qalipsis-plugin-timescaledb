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

package io.qalipsis.plugins.timescaledb

object TestUtils {

    /**
     * Generates a Fibonacci-series from a start, withing a range of values, which must not be Fibonacci numbers.
     */
    fun fibonacciFromRange(start: Int, end: Int): List<Int> {
        require(start >= 1) { "The start should be strictly positive" }
        require(end > start) { "The end should be greater than the start" }

        // Previous value in the series, initialized to 1.
        var previous = 1
        // Current value in the series, initialized to 2.
        var current = 2
        val result = mutableListOf<Int>()
        if (previous <= start) {
            result += previous
        }

        while (current <= end) {
            // Discard the values lower than the start.
            if (current >= start) {
                result += current
            }
            val newCurrent = current + previous
            previous = current
            current = newCurrent
        }

        return result
    }

    /**
     * Generates a Fibonacci-series from a start, with an expected size of the series.
     */
    fun fibonacciFromSize(start: Int, size: Int): List<Int> {
        require(start >= 1) { "The start should be strictly positive" }
        require(size > 1) { "The end should be greater than the start" }

        // Previous value in the series, initialized to 1.
        var previous = 1
        // Current value in the series, initialized to 2.
        var current = 2
        val result = mutableListOf<Int>()
        if (previous <= start) {
            result += previous
        }

        while (result.size < size) {
            // Discard the values lower than the start.
            if (current >= start) {
                result += current
            }
            val newCurrent = current + previous
            previous = current
            current = newCurrent
        }

        return result
    }
}