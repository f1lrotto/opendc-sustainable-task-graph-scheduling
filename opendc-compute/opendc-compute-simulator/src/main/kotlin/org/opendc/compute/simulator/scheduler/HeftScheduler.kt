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


public class HeftScheduler : ComputeScheduler {
    private val hosts = mutableListOf<HostView>()
    private val upwardRanks = mutableMapOf<Int, Double>()
    private val hostFinishTimes = mutableMapOf<HostView, Long>()
    private val taskFinishTimes = mutableMapOf<Int, Long>()
    private val taskAssignments = mutableMapOf<Int, HostView>()
    private val allTasks = mutableMapOf<Int, ServiceTask>()
    private val prioritizedTasks = mutableListOf<ServiceTask>()
    private var needsPriorityRecomputation = true

    override fun addHost(host: HostView) {
        hosts.add(host)
        hostFinishTimes[host] = 0L
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
        hostFinishTimes.remove(host)
        taskAssignments.values.removeAll { it == host }
    }

    override fun select(iter: MutableIterator<SchedulingRequest>): SchedulingResult {
        return select(iter, emptyList())
    }

    override fun select(iter: MutableIterator<SchedulingRequest>, blockedTasks: List<SchedulingRequest>): SchedulingResult {
        if (hosts.isEmpty()) {
            return SchedulingResult(SchedulingResultType.FAILURE)
        }

        // Collect all available tasks and add them to our task registry
        val availableTasks = mutableListOf<SchedulingRequest>()
        while (iter.hasNext()) {
            val req = iter.next()
            if (!req.isCancelled) {
                allTasks[req.task.id] = req.task
                availableTasks.add(req)
            }
        }

        // Also add blocked tasks to our task registry for complete DAG visibility
        for (req in blockedTasks) {
            if (!req.isCancelled) {
                allTasks[req.task.id] = req.task
            }
        }

        if (availableTasks.isEmpty()) {
            return SchedulingResult(SchedulingResultType.EMPTY)
        }

        // when new tasks arrive
        if (needsPriorityRecomputation) {
            recomputeTaskPriorities()
            needsPriorityRecomputation = false
        }

        val selectedRequest =
            selectHighestPriorityTask(availableTasks)
                ?: return SchedulingResult(SchedulingResultType.EMPTY)

        val task = selectedRequest.task

        val bestHost =
            selectBestHost(task)
                ?: return SchedulingResult(SchedulingResultType.FAILURE, null, selectedRequest)

        val finishTime = calculateEarliestFinishTime(task, bestHost)

        hostFinishTimes[bestHost] = finishTime
        taskFinishTimes[task.id] = finishTime
        taskAssignments[task.id] = bestHost

        availableTasks.remove(selectedRequest)
        return SchedulingResult(SchedulingResultType.SUCCESS, bestHost, selectedRequest)
    }

    override fun removeTask(
        task: ServiceTask,
        host: HostView?,
    ) {
        taskAssignments.remove(task.id)
        taskFinishTimes.remove(task.id)
        allTasks.remove(task.id)
        needsPriorityRecomputation = true
    }


    private fun recomputeTaskPriorities() {
        upwardRanks.clear()

        for (task in allTasks.values) {
            computeUpwardRank(task.id)
        }

        // Sort tasks by upward rank (descending order)
        prioritizedTasks.clear()
        prioritizedTasks.addAll(allTasks.values.sortedByDescending { upwardRanks[it.id] ?: 0.0 })
    }


    private fun selectHighestPriorityTask(availableTasks: List<SchedulingRequest>): SchedulingRequest? {
        // Convert available tasks to a set for quick lookup
        val availableTaskIds = availableTasks.map { it.task.id }.toSet()

        for (task in prioritizedTasks) {
            if (task.id in availableTaskIds) {
                return availableTasks.find { it.task.id == task.id }
            }
        }

        return availableTasks.firstOrNull()
    }


    private fun selectBestHost(task: ServiceTask): HostView? {
        var bestHost: HostView? = null
        var earliestFinishTime = Long.MAX_VALUE

        for (host in hosts) {
            if (!canHostTask(host, task)) {
                continue
            }

            val finishTime = calculateEarliestFinishTime(task, host)
            if (finishTime < earliestFinishTime) {
                earliestFinishTime = finishTime
                bestHost = host
            }
        }

        return bestHost
    }

    private fun canHostTask(
        host: HostView,
        task: ServiceTask,
    ): Boolean {
        val flavor = task.flavor
        val availableCores = host.host.getModel().coreCount - host.provisionedCpuCores
        return availableCores >= flavor.cpuCoreCount &&
            host.availableMemory >= flavor.memorySize
    }

    private fun calculateEarliestFinishTime(
        task: ServiceTask,
        host: HostView,
    ): Long {
        val earliestStartTime = calculateEarliestStartTime(task, host)

        val executionTime = estimateExecutionTime(task, host)

        return earliestStartTime + executionTime
    }

    private fun calculateEarliestStartTime(
        task: ServiceTask,
        host: HostView,
    ): Long {
        val parentFinishTime = getParentFinishTime(task)

        val hostAvailableTime = hostFinishTimes[host] ?: 0L

        return maxOf(parentFinishTime, hostAvailableTime)
    }


    private fun getParentFinishTime(task: ServiceTask): Long {
        val parents = task.flavor.parents
        if (parents.isEmpty()) {
            return 0L
        }

        // Find the maximum finish time among all parent tasks
        var maxParentFinishTime = 0L
        for (parentId in parents) {
            val parentFinishTime = taskFinishTimes[parentId] ?: 0L
            maxParentFinishTime = maxOf(maxParentFinishTime, parentFinishTime)
        }

        return maxParentFinishTime
    }

    private fun estimateExecutionTime(
        task: ServiceTask,
        host: HostView,
    ): Long {
        val flavor = task.flavor
        val taskCpuDemand = flavor.cpuCoreCount.toDouble()
        val hostCpuCapacity = host.host.getModel().coreCount.toDouble()

        val baseTime = task.duration.toMillis()

        // Adjust based on relative performance
        // If task needs more cores than available, it will take longer
        val performanceRatio = if (hostCpuCapacity > 0) taskCpuDemand / hostCpuCapacity else 1.0

        return (baseTime * performanceRatio).toLong()
    }


    private fun computeUpwardRank(taskId: Int): Double {
        // Return cached value if already computed
        if (upwardRanks.containsKey(taskId)) {
            return upwardRanks[taskId]!!
        }

        val task = allTasks[taskId]
        if (task == null) {
            upwardRanks[taskId] = 0.0
            return 0.0
        }

        val avgExecutionTime = estimateAverageExecutionTime(task)

        // Compute maximum upward rank of children plus communication cost
        val children = task.flavor.children
        var maxChildRank = 0.0

        for (childId in children) {
            val childTask = allTasks[childId]
            if (childTask != null) {
                // Recursively compute child's upward rank
                val childRank = computeUpwardRank(childId)
                // In a heterogeneous environment, we would add communication cost here
                // For now, assuming no communication cost as per OpenDC design
                maxChildRank = maxOf(maxChildRank, childRank)
            }
        }

        val rank = avgExecutionTime + maxChildRank
        upwardRanks[taskId] = rank

        return rank
    }

    private fun estimateAverageExecutionTime(task: ServiceTask): Double {
        if (hosts.isEmpty()) return task.duration.toMillis().toDouble()

        val totalTime =
            hosts.sumOf { host ->
                estimateExecutionTime(task, host).toDouble()
            }

        return totalTime / hosts.size
    }
}
