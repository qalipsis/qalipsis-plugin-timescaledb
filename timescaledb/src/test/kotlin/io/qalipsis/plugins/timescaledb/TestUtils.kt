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