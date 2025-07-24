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

import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask
import org.opendc.compute.simulator.service.SchedulingRequest
import org.opendc.compute.simulator.service.SchedulingResult
import org.opendc.compute.simulator.service.SchedulingResultType
import org.opendc.compute.simulator.scheduler.timeshift.Timeshifter
import org.opendc.simulator.Clock
import org.opendc.trace.util.Carbon.CarbonModel
import java.time.Instant


class HeftCarbonAwareScheduler(
    private val clock: Clock,
    private val filters: List<HostFilter> = emptyList(),
    private val subsetSize: Int = Int.MAX_VALUE,
    private val random: kotlin.random.Random = kotlin.random.Random.Default
) : HeftScheduler(), Timeshifter {

    // Carbon-aware timeshift state
    override var shortLowCarbon: Boolean = false
    override var longLowCarbon: Boolean = false
    override var carbonMod: CarbonModel? = null

    override fun select(iter: MutableIterator<SchedulingRequest>): SchedulingResult {
        if (hosts.isEmpty()) {
            return SchedulingResult(SchedulingResultType.FAILURE)
        }

        val availableTasks = mutableListOf<SchedulingRequest>()
        val deferrableTasks = mutableListOf<SchedulingRequest>()
        val criticalTasks = mutableListOf<SchedulingRequest>()

        while (iter.hasNext()) {
            val req = iter.next()
            if (!req.isCancelled) {
                allTasks[req.task.id] = req.task
                availableTasks.add(req)

                // Separate critical and deferrable tasks
                if (req.task.nature.deferrable) {
                    deferrableTasks.add(req)
                } else {
                    criticalTasks.add(req)
                }
            }
        }

        if (availableTasks.isEmpty()) {
            return SchedulingResult(SchedulingResultType.EMPTY)
        }

        if (needsPriorityRecomputation) {
            recomputeTaskPriorities()
            needsPriorityRecomputation = false
        }

        // First try to schedule critical tasks using HEFT
        val criticalResult = scheduleCriticalTasks(criticalTasks)
        if (criticalResult != null) {
            availableTasks.remove(criticalResult.request)
            return criticalResult
        }

        // Then, apply carbon-aware logic to deferrable tasks
        val deferrableResult = scheduleDefferrableTasks(deferrableTasks)
        if (deferrableResult != null) {
            availableTasks.remove(deferrableResult.request)
            return deferrableResult
        }

        return SchedulingResult(SchedulingResultType.EMPTY)
    }

    // Override canHostTask to include filters
    override fun canHostTask(host: HostView, task: ServiceTask): Boolean {
        val flavor = task.flavor
        val availableCores = host.host.getModel().coreCount - host.provisionedCpuCores
        return availableCores >= flavor.cpuCoreCount &&
            host.availableMemory >= flavor.memorySize &&
            filters.all { filter -> filter.test(host, task) }
    }

    private fun scheduleCriticalTasks(criticalTasks: List<SchedulingRequest>): SchedulingResult? {
        if (criticalTasks.isEmpty()) return null

        val selectedRequest = selectHighestPriorityTask(criticalTasks) ?: return null
        val task = selectedRequest.task

        val bestHost = selectBestHost(task) ?: return SchedulingResult(
            SchedulingResultType.FAILURE,
            null,
            selectedRequest
        )

        val finishTime = calculateEarliestFinishTime(task, bestHost)
        updateTaskAssignment(task, bestHost, finishTime)

        return SchedulingResult(SchedulingResultType.SUCCESS, bestHost, selectedRequest)
    }

    private fun scheduleDefferrableTasks(deferrableTasks: List<SchedulingRequest>): SchedulingResult? {
        for (req in deferrableTasks) {
            val task = req.task

            if (shouldDeferTask(task)) {
                // Skip this task - it will be reconsidered in next scheduling cycle
                continue
            }

            // If we reach here, either it's low carbon period or deadline doesn't permit deferral
            // Use HEFT logic
            val bestHost = selectBestHost(task) ?: continue

            val finishTime = calculateEarliestFinishTime(task, bestHost)
            updateTaskAssignment(task, bestHost, finishTime)

            return SchedulingResult(SchedulingResultType.SUCCESS, bestHost, req)
        }

        return null
    }

    private fun shouldDeferTask(task: ServiceTask): Boolean {
        val durInHours = task.duration.toHours()
        val isHighCarbonPeriod = if (durInHours < 2) !shortLowCarbon else !longLowCarbon

        if (!isHighCarbonPeriod) {
            // Low carbon period - schedule immediately
            return false
        }

        // High carbon period - check if we can defer without violating deadline
        val currentTime = clock.instant()
        val estimatedCompletion = currentTime.plus(task.duration)
        val deadline = Instant.ofEpochMilli(task.deadline)

        // Only defer if we have sufficient time before deadline
        return estimatedCompletion.isBefore(deadline)
    }

    override fun updateCarbonIntensity(carbonIntensity: Double) {
        carbonMod?.updateCarbonIntensity(carbonIntensity)
        
        // Update carbon regime flags based on current intensity and thresholds
        carbonMod?.let { model ->
            shortLowCarbon = model.isLowCarbonIntensity(2.0)
            longLowCarbon = model.isLowCarbonIntensity(8.0)
        }
    }
}
