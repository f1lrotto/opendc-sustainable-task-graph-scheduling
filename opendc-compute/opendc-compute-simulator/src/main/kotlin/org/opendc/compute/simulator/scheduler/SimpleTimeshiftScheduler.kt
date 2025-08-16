/*
 * Copyright (c) 2025 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.compute.simulator.scheduler

import org.opendc.compute.simulator.scheduler.filters.HostFilter
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask
import org.opendc.simulator.compute.power.CarbonModel
import org.opendc.simulator.compute.power.CarbonReceiver
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import java.util.SplittableRandom
import java.util.random.RandomGenerator

/**
 * Simple carbon-aware scheduler that only performs time-shifting.
 *
 * It ignores workflow/critical-path optimizations and delays each task to the
 * lowest-carbon forecast window within a fixed deferral budget. Host selection
 * is done using HEFT's earliest-finish rule from [HeftScheduler].
 */
public class SimpleTimeshiftScheduler(
    public val clock: InstantSource,
    private val filters: List<HostFilter>,
    private val random: RandomGenerator = SplittableRandom(0),
    private val forecastSize: Int = 24,
    private val maxSkipsPerTask: Int = 5,
    private val maxDeferral: Duration = Duration.ofHours(24),
) : HeftScheduler(), CarbonReceiver {
    private var carbonModel: CarbonModel? = null

    override fun updateCarbonIntensity(carbonIntensity: Double) {
        // No smoothing/threshold logic in the simple version.
    }

    override fun setCarbonModel(carbonModel: CarbonModel) {
        this.carbonModel = carbonModel
    }

    override fun removeCarbonModel(carbonModel: CarbonModel) {
        if (this.carbonModel === carbonModel) this.carbonModel = null
    }

    override fun select(iter: MutableIterator<SchedulingRequest>): SchedulingResult {
        return select(iter, emptyList())
    }

    override fun select(
        iter: MutableIterator<SchedulingRequest>,
        blockedTasks: List<SchedulingRequest>,
    ): SchedulingResult {
        if (hosts.isEmpty()) {
            return SchedulingResult(SchedulingResultType.FAILURE)
        }

        val now = clock.instant()

        // Gather available requests and register tasks for ranking
        val availableRequests = mutableListOf<SchedulingRequest>()
        while (iter.hasNext()) {
            val req = iter.next()
            val task = req.task
            if (req.isCancelled || !isTaskSchedulable(task)) {
                iter.remove()
                continue
            }
            if (!allTasks.containsKey(task.id)) {
                allTasks[task.id] = task
                needsPriorityRecomputation = true
            } else {
                allTasks[task.id] = task
            }
            availableRequests.add(req)
        }

        if (availableRequests.isEmpty()) {
            return SchedulingResult(SchedulingResultType.EMPTY)
        }

        if (needsPriorityRecomputation) {
            recomputeTaskPriorities()
            needsPriorityRecomputation = false
        }

        val reqByTaskId = availableRequests.associateBy { it.task.id }

        var chosenReq: SchedulingRequest? = null
        var chosenHost: HostView? = null

        for (task in prioritizedTasks) {
            val req = reqByTaskId[task.id] ?: continue

            // Respect any prior deferral decision
            val due = req.deferUntil
            if (due != null && now.isBefore(due)) {
                continue
            }

            // Simple time-shifting: look from 'now' up to max deferral for the greenest window
            val cm = carbonModel
            if (cm != null && req.timesSkipped < maxSkipsPerTask) {
                val bestStart = bestGreenStart(now, task.duration.toMillis(), cm)
                if (bestStart != null && now.isBefore(bestStart) && Duration.between(now, bestStart) <= maxDeferral) {
                    req.deferUntil = bestStart
                    req.timesSkipped += 1
                    continue
                }
            }

            // Filter candidate hosts and pick earliest-finish (HEFT rule)
            val filteredHosts =
                hosts.filter { host ->
                    filters.all { it.test(host, task) } && canHostTask(host, task)
                }
            if (filteredHosts.isEmpty()) {
                continue
            }

            var best: HostView? = null
            var bestFinish = Long.MAX_VALUE
            for (h in filteredHosts) {
                val finish = calculateEarliestFinishTime(task, h)
                if (finish < bestFinish) {
                    bestFinish = finish
                    best = h
                }
            }

            if (best != null) {
                updateTaskAssignment(task, best, bestFinish)
                chosenReq = req
                chosenHost = best
                break
            }
        }

        if (chosenReq == null || chosenHost == null) {
            return SchedulingResult(SchedulingResultType.FAILURE, null, availableRequests.first())
        }

        chosenReq.isCancelled = true
        return SchedulingResult(SchedulingResultType.SUCCESS, chosenHost, chosenReq)
    }

    override fun removeTask(
        task: ServiceTask,
        host: HostView?,
    ) {
        super.removeTask(task, host)
        needsPriorityRecomputation = true
    }

    private fun bestGreenStart(
        est: Instant,
        durationMillis: Long,
        cm: CarbonModel,
    ): Instant? {
        val series = cm.getForecast(forecastSize)
        if (series.isEmpty()) return null

        val stepMillis = cm.forecastStepMillis
        val spanSteps = ((durationMillis + stepMillis - 1) / stepMillis).toInt().coerceAtLeast(1)
        if (series.size < spanSteps) return null

        val estMillis = est.toEpochMilli()
        val latestStartMillis = estMillis + maxDeferral.toMillis() - durationMillis
        if (latestStartMillis < estMillis) return null

        val step = stepMillis.toLong()
        val seriesStart = ((estMillis + step - 1) / step) * step

        val windowSteps = ((latestStartMillis - seriesStart) / step).toInt().coerceAtLeast(0)
        val maxOffset = kotlin.math.min(windowSteps, series.size - spanSteps)
        if (maxOffset < 0) return null

        fun sumAt(offset: Int): Double {
            var s = 0.0
            for (k in 0 until spanSteps) s += series[offset + k]
            return s
        }

        var bestOffset = 0
        var bestVal = sumAt(0)
        for (i in 0..maxOffset) {
            val v = sumAt(i)
            if (v < bestVal || (v == bestVal && i > bestOffset)) {
                bestVal = v
                bestOffset = i
            }
        }

        return Instant.ofEpochMilli(seriesStart + bestOffset.toLong() * step)
    }
}


