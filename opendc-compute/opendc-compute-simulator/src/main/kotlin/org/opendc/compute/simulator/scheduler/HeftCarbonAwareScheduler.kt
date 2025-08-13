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
import java.util.LinkedList
import java.util.SplittableRandom
import java.util.random.RandomGenerator

/**
 * Carbon-aware HEFT scheduler.
 *
 * This scheduler keeps HEFT's task priority logic but defers deferrable tasks during
 * high-carbon periods within soft deadline constraints. It also respects host filters.
 */
public class HeftCarbonAwareScheduler(
    public val clock: InstantSource,
    private val filters: List<HostFilter>,
    private val random: RandomGenerator = SplittableRandom(0),
    // Carbon-awareness parameters (defaults aligned with Timeshift scheduler behavior)
    private val forecast: Boolean = true,
    private val shortForecastThreshold: Double = 0.20,
    private val longForecastThreshold: Double = 0.35,
    private val forecastSize: Int = 24,
    private val windowSize: Int = 24,
    // Guardrails to limit excessive deferral
    private val maxSkipsPerTask: Int = 6,
    // Soft makespan stretch vs baseline HEFT
    private val rho: Double = 1.05,
    // Planning knobs
    private val lockHorizon: Duration = Duration.ofHours(3),
    private val replanEvery: Duration = Duration.ofMinutes(60),
    private val minGain_gCO2: Double = 0.0,
    private val maxDeferralPerEpoch: Duration = Duration.ofHours(2),
) : HeftScheduler(), CarbonReceiver {
    // Carbon model state (simplified copy of Timeshifter logic)
    private var carbonMod: CarbonModel? = null
    private val pastCarbonIntensities: LinkedList<Double> = LinkedList()
    private var carbonRunningSum: Double = 0.0
    private var shortLowCarbon: Boolean = false
    private var longLowCarbon: Boolean = false

    // Baseline/makespan windows (minimal viable scaffolding)
    private var lastReplanAt: Instant = Instant.EPOCH
    private val latestStartTime: MutableMap<Int, Instant> = mutableMapOf()

    /**
     * CarbonReceiver hook.
     */
    override fun updateCarbonIntensity(newCarbonIntensity: Double) {
        if (!forecast) {
            noForecastUpdateCarbonIntensity(newCarbonIntensity)
            return
        }

        val cm = carbonMod ?: return
        val forecastValues = cm.getForecast(forecastSize)
        if (forecastValues.isEmpty()) return

        val sorted = forecastValues.sorted()
        val shortIdx = (sorted.size * shortForecastThreshold).toInt().coerceIn(0, sorted.lastIndex)
        val longIdx = (sorted.size * longForecastThreshold).toInt().coerceIn(0, sorted.lastIndex)
        val shortCI = sorted[shortIdx]
        val longCI = sorted[longIdx]

        shortLowCarbon = newCarbonIntensity < shortCI
        longLowCarbon = newCarbonIntensity < longCI
    }

    override fun setCarbonModel(carbonModel: CarbonModel) {
        this.carbonMod = carbonModel
    }

    override fun removeCarbonModel(carbonModel: CarbonModel) {
        if (this.carbonMod === carbonModel) this.carbonMod = null
    }

    private fun noForecastUpdateCarbonIntensity(newCarbonIntensity: Double) {
        val previous = if (pastCarbonIntensities.isEmpty()) 0.0 else pastCarbonIntensities.last
        pastCarbonIntensities.addLast(newCarbonIntensity)
        carbonRunningSum += newCarbonIntensity
        if (pastCarbonIntensities.size > windowSize) {
            carbonRunningSum -= pastCarbonIntensities.removeFirst()
        }
        val threshold = if (pastCarbonIntensities.isEmpty()) newCarbonIntensity else carbonRunningSum / pastCarbonIntensities.size
        shortLowCarbon = (newCarbonIntensity < threshold) && (newCarbonIntensity > previous)
        longLowCarbon = (newCarbonIntensity < threshold)
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

        // Collect candidates from queue, removing cancelled and non-schedulable requests
        val availableRequests = mutableListOf<SchedulingRequest>()
        while (iter.hasNext()) {
            val req = iter.next()
            val task = req.task
            if (req.isCancelled || !isTaskSchedulable(task)) {
                iter.remove()
                continue
            }
            allTasks[task.id] = task
            availableRequests.add(req)
        }

        // Also add blocked tasks for complete DAG context
        for (req in blockedTasks) {
            if (!req.isCancelled) {
                allTasks[req.task.id] = req.task
            }
        }

        if (availableRequests.isEmpty()) {
            return SchedulingResult(SchedulingResultType.EMPTY)
        }

        // Periodic (re)compute priorities and slack windows stub
        if (lastReplanAt == Instant.EPOCH || Duration.between(lastReplanAt, now) >= replanEvery) {
            recomputeTaskPriorities()
            computeLatestStartsStub(now)
            lastReplanAt = now
        }

        // Build a map for quick lookup from task id to request
        val reqByTaskId = availableRequests.associateBy { it.task.id }

        // Iterate prioritized tasks and find first carbon-feasible candidate
        var chosenReq: SchedulingRequest? = null
        var chosenHost: HostView? = null

        for (task in prioritizedTasks) {
            val req = reqByTaskId[task.id] ?: continue

            // Honor previous defer-until
            val due = req.deferUntil
            if (due != null && now.isBefore(due)) {
                continue
            }

            // Carbon-aware temporal shifting for deferrable tasks using arg-min valley
            if (task.nature.deferrable) {
                val deadline = Instant.ofEpochMilli(task.deadline)
                // Window [EST, LST]
                val est = maxOf(getParentReadyTime(task), now)
                val lst = latestStartTime[task.id] ?: minOf(deadline.minus(task.duration), now.plus(maxDeferralPerEpoch))

                if (!est.plus(task.duration).isAfter(lst)) {
                    val choice = bestGreenStart(task, est, lst, carbonMod)
                    if (choice != null) {
                        val outsideLock = now.isBefore(choice.start.minus(lockHorizon))
                        val withinCap = Duration.between(now, choice.start) <= maxDeferralPerEpoch
                        val critical = isHighRank(task)
                        val needGain = if (critical) minGain_gCO2 * 2 else minGain_gCO2
                        if (outsideLock && withinCap && choice.gain_gCO2 >= needGain && req.timesSkipped < maxSkipsPerTask) {
                            req.deferUntil = choice.start.minus(lockHorizon)
                            req.timesSkipped += 1
                            continue
                        }
                    }
                }
            }

            // Filter candidate hosts with configured filters and capacity
            val filteredHosts =
                hosts.filter { host ->
                    filters.all { it.test(host, task) } && canHostTask(host, task)
                }

            if (filteredHosts.isEmpty()) {
                // Keep searching for another task; if none found, report failure for this req
                continue
            }

            // Choose the host that minimizes earliest finish time (HEFT)
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
                // Reserve times internally
                updateTaskAssignment(task, best, calculateEarliestFinishTime(task, best))
                chosenReq = req
                chosenHost = best
                break
            }
        }

        if (chosenReq == null || chosenHost == null) {
            // No feasible placement now
            return SchedulingResult(SchedulingResultType.FAILURE, null, availableRequests.first())
        }

        // Mark selected request as consumed to avoid double-scheduling in future cycles
        chosenReq.isCancelled = true

        return SchedulingResult(SchedulingResultType.SUCCESS, chosenHost, chosenReq)
    }

    override fun removeTask(
        task: ServiceTask,
        host: HostView?,
    ) {
        super.removeTask(task, host)
        // Trigger re-prioritization on next cycle
        needsPriorityRecomputation = true
    }

    private fun computeLatestStartsStub(now: Instant) {
        // Minimal placeholder: derive LST from per-task deadline if present; else allow modest slack
        for ((id, task) in allTasks) {
            val deadlineInst = Instant.ofEpochMilli(task.deadline)
            val lstFromDeadline = deadlineInst.minus(task.duration)
            val lstFromCap = now.plus(maxDeferralPerEpoch)
            latestStartTime[id] = minOf(lstFromDeadline, lstFromCap)
        }
    }

    private fun getParentReadyTime(task: ServiceTask): Instant {
        val parents = task.flavor.parents
        if (parents.isEmpty()) return Instant.EPOCH
        var maxFinish = 0L
        for (p in parents) {
            val ft = taskFinishTimes[p] ?: 0L
            if (ft > maxFinish) maxFinish = ft
        }
        return Instant.ofEpochMilli(maxFinish)
    }

    private fun isHighRank(task: ServiceTask): Boolean {
        val rank = upwardRanks[task.id] ?: return false
        // Simple top-quantile check
        if (upwardRanks.isEmpty()) return false
        val sorted = upwardRanks.values.sortedDescending()
        val cutoff = sorted[(sorted.size * 0.2).coerceAtLeast(0.0).toInt().coerceAtMost(sorted.lastIndex)]
        return rank >= cutoff
    }

    private data class GreenChoice(val start: Instant, val gain_gCO2: Double)

    private fun bestGreenStart(
        task: ServiceTask,
        est: Instant,
        lst: Instant,
        cm: CarbonModel?,
    ): GreenChoice? {
        cm ?: return null
        val series = cm.getForecast(forecastSize)
        if (series.isEmpty()) return null

        val durMillis = task.duration.toMillis()
        val stepMillis = cm.forecastStepMillis
        val anchorMillis = cm.getForecastAnchorTimeMillis()

        // Baseline: if we start at est, compute its integral as comparison
        fun integralAt(offsetSteps: Int): Double {
            // Approximate integral by summing discrete CI over steps covered by duration
            val steps = ((durMillis + stepMillis - 1) / stepMillis).toInt()
            var sum = 0.0
            for (k in 0 until steps) {
                val idx = (offsetSteps + k).coerceIn(0, series.size - 1)
                sum += series[idx]
            }
            return sum
        }

        // Find bounds in steps between est..lst-dur
        val maxStart = lst.minusMillis(durMillis)
        if (maxStart.isBefore(est)) return null

        // Align to forecast anchor
        val startOffsetSteps = ((est.toEpochMilli() - anchorMillis) / stepMillis).toInt()
        val totalSteps = (Duration.between(est, maxStart).toMillis() / stepMillis).toInt().coerceAtLeast(0)

        val baseline = integralAt(startOffsetSteps)
        var bestIdx = 0
        var bestVal = baseline
        for (s in 0..totalSteps) {
            val v = integralAt(startOffsetSteps + s)
            if (v < bestVal) {
                bestVal = v
                bestIdx = s
            }
        }

        val bestStart = est.plusMillis(bestIdx.toLong() * stepMillis)
        val gain = (baseline - bestVal)
        return GreenChoice(bestStart, gain)
    }
}
