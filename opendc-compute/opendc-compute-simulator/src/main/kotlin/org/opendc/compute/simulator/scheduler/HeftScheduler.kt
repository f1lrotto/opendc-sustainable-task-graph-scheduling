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

import org.opendc.compute.api.TaskState
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask

public open class HeftScheduler : ComputeScheduler {
    protected val hosts: MutableList<HostView> = mutableListOf<HostView>()
    protected val upwardRanks: MutableMap<Int, Double> = mutableMapOf<Int, Double>()
    protected val hostFinishTimes: MutableMap<HostView, Long> = mutableMapOf<HostView, Long>()
    protected val taskFinishTimes: MutableMap<Int, Long> = mutableMapOf<Int, Long>()
    protected val taskAssignments: MutableMap<Int, HostView> = mutableMapOf<Int, HostView>()
    protected val allTasks: MutableMap<Int, ServiceTask> = mutableMapOf<Int, ServiceTask>()
    protected val prioritizedTasks: MutableList<ServiceTask> = mutableListOf<ServiceTask>()
    protected var needsPriorityRecomputation: Boolean = true

    /**
     * Per-host allocation timeline to compute earliest feasible start times under capacity constraints.
     */
    private val hostSchedules: MutableMap<HostView, MutableList<Allocation>> = mutableMapOf()
    private val taskStartTimes: MutableMap<Int, Long> = mutableMapOf()

    /**
     * An allocation interval on a host.
     */
    private data class Allocation(
        val taskId: Int,
        val start: Long,
        val end: Long,
        val cpuCores: Int,
        val memory: Long,
    )

    override fun addHost(host: HostView) {
        hosts.add(host)
        hostFinishTimes[host] = 0L
        hostSchedules[host] = mutableListOf()
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
        hostFinishTimes.remove(host)
        taskAssignments.values.removeAll { it == host }
        hostSchedules.remove(host)
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

        // Collect all available tasks and add them to our task registry
        val availableTasks = mutableListOf<SchedulingRequest>()
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
            availableTasks.add(req)
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

        val (startTime, finishTime) = calculateEarliestStartAndFinishTimes(task, bestHost)

        // Book internal resources
        updateTaskAssignment(task, bestHost, startTime, finishTime)

        // Mark request as consumed; it will be removed from the queue in the next iteration
        selectedRequest.isCancelled = true
        return SchedulingResult(SchedulingResultType.SUCCESS, bestHost, selectedRequest)
    }

    override fun removeTask(
        task: ServiceTask,
        host: HostView?,
    ) {
        val assignedHost = host ?: taskAssignments[task.id]
        if (assignedHost != null) {
            val sched = hostSchedules[assignedHost]
            if (sched != null) {
                val it = sched.iterator()
                while (it.hasNext()) {
                    val a = it.next()
                    if (a.taskId == task.id) {
                        it.remove()
                        break
                    }
                }
                // Recompute host finish time as the latest end across remaining allocations
                hostFinishTimes[assignedHost] = sched.maxOfOrNull { it.end } ?: 0L
            }
        }
        taskAssignments.remove(task.id)
        taskFinishTimes.remove(task.id)
        taskStartTimes.remove(task.id)
        allTasks.remove(task.id)
        needsPriorityRecomputation = true
    }

    protected fun recomputeTaskPriorities() {
        upwardRanks.clear()

        for (task in allTasks.values) {
            computeUpwardRank(task.id)
        }

        // Sort tasks by upward rank (descending order)
        prioritizedTasks.clear()
        prioritizedTasks.addAll(allTasks.values.sortedByDescending { upwardRanks[it.id] ?: 0.0 })
    }

    protected fun selectHighestPriorityTask(availableTasks: List<SchedulingRequest>): SchedulingRequest? {
        // Convert available tasks to a set for quick lookup
        val availableTaskIds = availableTasks.map { it.task.id }.toSet()

        for (task in prioritizedTasks) {
            if (task.id in availableTaskIds) {
                return availableTasks.find { it.task.id == task.id }
            }
        }

        return availableTasks.firstOrNull()
    }

    protected fun selectBestHost(task: ServiceTask): HostView? {
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

    protected fun canHostTask(
        host: HostView,
        task: ServiceTask,
    ): Boolean {
        val flavor = task.flavor
        val availableCores = host.host.getModel().coreCount - host.provisionedCpuCores
        return availableCores >= flavor.cpuCoreCount && host.availableMemory >= flavor.memorySize
    }

    protected fun calculateEarliestFinishTime(
        task: ServiceTask,
        host: HostView,
    ): Long {
        val (start, finish) = calculateEarliestStartAndFinishTimes(task, host)
        return finish
    }

    private fun calculateEarliestStartAndFinishTimes(
        task: ServiceTask,
        host: HostView,
    ): Pair<Long, Long> {
        val start = calculateEarliestStartTime(task, host)
        val finish = start + estimateExecutionTime(task, host)
        return start to finish
    }

    protected fun calculateEarliestStartTime(
        task: ServiceTask,
        host: HostView,
    ): Long {
        val parentFinishTime = getParentFinishTime(task)
        val duration = estimateExecutionTime(task, host)
        val sched = hostSchedules[host] ?: mutableListOf()

        val requiredCores = task.flavor.cpuCoreCount
        val requiredMem = task.flavor.memorySize
        // Effective capacity left on the host right now (excluding what is already provisioned)
        val capacityCores = host.host.getModel().coreCount - host.provisionedCpuCores
        val capacityMem = host.availableMemory

        if (sched.isEmpty()) {
            return parentFinishTime
        }

        // Candidate start times: parent ready time and all allocation end times at/after it
        val candidates = mutableListOf<Long>()
        candidates.add(parentFinishTime)
        for (a in sched) {
            if (a.end >= parentFinishTime) candidates.add(a.end)
        }
        candidates.sort()

        for (start in candidates) {
            val end = start + duration
            if (isWindowFeasible(sched, start, end, requiredCores, requiredMem, capacityCores, capacityMem)) {
                return start
            }
        }

        // If nothing fit within existing gaps, start after the latest allocation finishes
        val lastEnd = maxOf(parentFinishTime, sched.maxOfOrNull { it.end } ?: 0L)
        return lastEnd
    }

    protected fun getParentFinishTime(task: ServiceTask): Long {
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

    protected fun estimateExecutionTime(
        task: ServiceTask,
        host: HostView,
    ): Long {
        // A task's wall-clock duration is specified by its workload and does not
        // shrink on hosts with more cores. Core count is used only for feasibility
        // (can the host accommodate the concurrent cores), not for speedup.
        return task.duration.toMillis()
    }

    private fun isWindowFeasible(
        allocations: List<Allocation>,
        start: Long,
        end: Long,
        reqCores: Int,
        reqMem: Long,
        capacityCores: Int,
        capacityMem: Long,
    ): Boolean {
        // Compute resource usage at start
        var usedCores = 0
        var usedMem = 0L
        val events = mutableListOf<Pair<Long, AllocationEvent>>()
        for (a in allocations) {
            if (a.end <= start || a.start >= end) continue // no overlap with [start, end)
            if (a.start <= start && a.end > start) {
                usedCores += a.cpuCores
                usedMem += a.memory
            }
            val s = maxOf(a.start, start)
            val e = minOf(a.end, end)
            // Register events inside (start, end)
            if (s > start) events.add(s to AllocationEvent(deltaCores = a.cpuCores, deltaMem = a.memory, entering = true))
            if (e > start) events.add(e to AllocationEvent(deltaCores = a.cpuCores, deltaMem = a.memory, entering = false))
        }
        if (usedCores + reqCores > capacityCores || usedMem + reqMem > capacityMem) return false

        // Sort events and sweep
        events.sortWith(compareBy<Pair<Long, AllocationEvent>> { it.first }.thenBy { if (it.second.entering) 0 else 1 })
        for ((_, ev) in events) {
            if (ev.entering) {
                usedCores += ev.deltaCores
                usedMem += ev.deltaMem
            } else {
                usedCores -= ev.deltaCores
                usedMem -= ev.deltaMem
            }
            if (usedCores + reqCores > capacityCores || usedMem + reqMem > capacityMem) return false
        }
        return true
    }

    private data class AllocationEvent(val deltaCores: Int, val deltaMem: Long, val entering: Boolean)

    protected fun computeUpwardRank(taskId: Int): Double {
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

    protected fun estimateAverageExecutionTime(task: ServiceTask): Double {
        if (hosts.isEmpty()) return task.duration.toMillis().toDouble()

        val totalTime =
            hosts.sumOf { host ->
                estimateExecutionTime(task, host).toDouble()
            }

        return totalTime / hosts.size
    }

    protected fun updateTaskAssignment(
        task: ServiceTask,
        host: HostView,
        finishTime: Long,
    ) {
        // Backward-compatible overload: infer start from finish and duration, and book resources
        val startTime = (finishTime - estimateExecutionTime(task, host)).coerceAtLeast(0L)
        updateTaskAssignment(task, host, startTime, finishTime)
    }

    protected fun updateTaskAssignment(
        task: ServiceTask,
        host: HostView,
        startTime: Long,
        finishTime: Long,
    ) {
        val sched = hostSchedules.computeIfAbsent(host) { mutableListOf() }
        sched.add(
            Allocation(
                taskId = task.id,
                start = startTime,
                end = finishTime,
                cpuCores = task.flavor.cpuCoreCount,
                memory = task.flavor.memorySize,
            ),
        )
        // Keep schedule sorted to speed up searches
        sched.sortBy { it.start }

        hostFinishTimes[host] = sched.maxOfOrNull { it.end } ?: finishTime
        taskStartTimes[task.id] = startTime
        taskFinishTimes[task.id] = finishTime
        taskAssignments[task.id] = host
    }

    /**
     * Check if a task is in a state that allows it to be scheduled.
     * Tasks that are already running, completed, terminated, or failed should not be scheduled again.
     */
    protected fun isTaskSchedulable(task: ServiceTask): Boolean {
        return when (task.state) {
            TaskState.CREATED,
            TaskState.PROVISIONING,
            -> true
            TaskState.RUNNING,
            TaskState.COMPLETED,
            TaskState.TERMINATED,
            TaskState.FAILED,
            TaskState.PAUSED,
            TaskState.DELETED,
            -> false
        }
    }
}
