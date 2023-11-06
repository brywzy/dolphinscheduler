package org.apache.dolphinscheduler.dao.utils;

import org.apache.dolphinscheduler.dao.entity.ProcessInstance;

public class ProcessInstanceUtils {

    /**
     * Copy the property of given source {@link ProcessInstance} to target.
     *
     * @param source Given process instance, copy from.
     * @param target Given process instance, copy to
     * @return a soft copy of given task instance.
     */
    public static void copyProcessInstance(ProcessInstance source, ProcessInstance target) {
        target.setId(source.getId());
        target.setProcessDefinitionCode(source.getProcessDefinitionCode());
        target.setProcessDefinitionVersion(source.getProcessDefinitionVersion());
        target.setState(source.getState());
        target.setStateHistory(source.getStateHistory());
        target.setStateDescList(source.getStateDescList());
        target.setRecovery(source.getRecovery());
        target.setStartTime(source.getStartTime());
        target.setEndTime(source.getEndTime());
        target.setRunTimes(source.getRunTimes());
        target.setName(source.getName());
        target.setHost(source.getHost());
        target.setProcessDefinition(source.getProcessDefinition());
        target.setCommandType(source.getCommandType());
        target.setCommandParam(source.getCommandParam());
        target.setTaskDependType(source.getTaskDependType());
        target.setMaxTryTimes(source.getMaxTryTimes());
        target.setFailureStrategy(source.getFailureStrategy());
        target.setWarningType(source.getWarningType());
        target.setWarningGroupId(source.getWarningGroupId());
        target.setScheduleTime(source.getScheduleTime());
        target.setCommandStartTime(source.getStartTime());
        target.setGlobalParams(source.getGlobalParams());
        target.setDagData(source.getDagData());
        target.setExecutorId(source.getExecutorId());
        target.setExecutorName(source.getExecutorName());
        target.setTenantCode(source.getTenantCode());
        target.setQueue(source.getQueue());
        target.setIsSubProcess(source.getIsSubProcess());
        target.setLocations(source.getLocations());
        target.setHistoryCmd(source.getHistoryCmd());
        target.setDependenceScheduleTimes(source.getDependenceScheduleTimes());
        target.setDuration(source.getDuration());
        target.setProcessInstancePriority(source.getProcessInstancePriority());
        target.setWorkerGroup(source.getWorkerGroup());
        target.setEnvironmentCode(source.getEnvironmentCode());
        target.setTimeout(source.getTimeout());
        target.setTenantId(source.getTenantId());
        //todo 针对 object类型太大
//        target.setVarPool(source.getVarPool());
        target.setNextProcessInstanceId(source.getNextProcessInstanceId());
        target.setDryRun(source.getDryRun());
        target.setRestartTime(source.getRestartTime());
        target.setBlocked(source.isBlocked());
    }
}
