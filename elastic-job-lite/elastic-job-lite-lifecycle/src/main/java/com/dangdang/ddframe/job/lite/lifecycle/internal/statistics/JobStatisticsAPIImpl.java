/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.lite.lifecycle.internal.statistics;

import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.LiteJobConfigurationGsonFactory;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;
import com.dangdang.ddframe.job.lite.lifecycle.api.JobStatisticsAPI;
import com.dangdang.ddframe.job.lite.lifecycle.domain.JobBriefInfo;
import com.dangdang.ddframe.job.lite.lifecycle.domain.JobBriefInfo.JobStatus;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 作业状态展示的实现类.
 *
 * @author caohao
 */
@RequiredArgsConstructor
public final class JobStatisticsAPIImpl implements JobStatisticsAPI {
    
    private final CoordinatorRegistryCenter regCenter;
    
    @Override
    public int getJobsTotalCount() {
        return this.getJobsTotalCount("");
    }

    @Override
    public int getJobsTotalCount(String userName) {
        return regCenter.getChildrenKeys("/" + userName).size();
    }

    @Override
    public Collection<JobBriefInfo> getAllJobsBriefInfo() {
        return this.getAllJobsBriefInfo("");
    }

    @Override
    public Collection<JobBriefInfo> getAllJobsBriefInfo(String userName) {
        List<String> jobNames = regCenter.getChildrenKeys("/" + userName);
        List<JobBriefInfo> result = new ArrayList<>(jobNames.size());
        for (String each : jobNames) {
            JobBriefInfo jobBriefInfo = getJobBriefInfo(each, userName);
            if (null != jobBriefInfo) {
                result.add(jobBriefInfo);
            }
        }
        Collections.sort(result);
        return result;
    }

    @Override
    public JobBriefInfo getJobBriefInfo(final String jobName) {
        return this.getJobBriefInfo(jobName, "");
    }

    @Override
    public JobBriefInfo getJobBriefInfo(final String jobName, String userName) {
        JobNodePath jobNodePath = new JobNodePath(jobName);
        JobBriefInfo result = new JobBriefInfo();
        result.setJobName(jobName);
        String liteJobConfigJson = regCenter.get(jobNodePath.getConfigNodePath(userName));
        if (null == liteJobConfigJson) {
            return null;
        }
        LiteJobConfiguration liteJobConfig = LiteJobConfigurationGsonFactory.fromJson(liteJobConfigJson);
        result.setDescription(liteJobConfig.getTypeConfig().getCoreConfig().getDescription());
        result.setCron(liteJobConfig.getTypeConfig().getCoreConfig().getCron());
        result.setInstanceCount(getJobInstanceCount(jobName));
        result.setShardingTotalCount(liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount());
        result.setStatus(getJobStatus(jobName, userName));
        return result;
    }
    
    private JobStatus getJobStatus(final String jobName, String userName) {
        JobNodePath jobNodePath = new JobNodePath(jobName);
        List<String> instances = regCenter.getChildrenKeys(jobNodePath.getInstancesNodePath(userName));
        if (instances.isEmpty()) {
            return JobStatus.CRASHED;
        }
        if (isAllDisabled(jobNodePath, userName)) {
            return JobStatus.DISABLED;
        }
        if (isHasShardingFlag(jobNodePath, instances, userName)) {
            return JobStatus.SHARDING_FLAG;
        }
        return JobStatus.OK;
    }

    private boolean isAllDisabled(final JobNodePath jobNodePath, final String userName) {
        List<String> serversPath = regCenter.getChildrenKeys(jobNodePath.getServerNodePathByUserName(userName));
        int disabledServerCount = 0;
        for (String each : serversPath) {
            if (JobStatus.DISABLED.name().equals(regCenter.get(jobNodePath.getServerNodePath(each, userName)))) {
                disabledServerCount++;
            }
        }
        return disabledServerCount == serversPath.size();
    }
    
    private boolean isHasShardingFlag(final JobNodePath jobNodePath, final List<String> instances, final String userName) {
        Set<String> shardingInstances = new HashSet<>();
        for (String each : regCenter.getChildrenKeys(jobNodePath.getShardingNodePath(userName))) {
            String instanceId = regCenter.get(jobNodePath.getShardingNodePath(each, "instance", userName));
            if (null != instanceId && !instanceId.isEmpty()) {
                shardingInstances.add(instanceId);
            }
        }
        return !instances.containsAll(shardingInstances) || shardingInstances.isEmpty();
    }
    
    private int getJobInstanceCount(final String jobName) {
        return regCenter.getChildrenKeys(new JobNodePath(jobName).getInstancesNodePath()).size();
    }
    
    @Override
    public Collection<JobBriefInfo> getJobsBriefInfo(final String ip) {
        return this.getJobsBriefInfo(ip, "");
    }

    @Override
    public Collection<JobBriefInfo> getJobsBriefInfo(String ip, String username) {
        List<String> jobNames = regCenter.getChildrenKeys("/" + username);
        List<JobBriefInfo> result = new ArrayList<>(jobNames.size());
        for (String each : jobNames) {
            JobBriefInfo jobBriefInfo = getJobBriefInfoByJobNameAndIp(each, ip, username);
            if (null != jobBriefInfo) {
                result.add(jobBriefInfo);
            }
        }
        Collections.sort(result);
        return result;
    }

    private JobBriefInfo getJobBriefInfoByJobNameAndIp(final String jobName, final String ip, String username) {
        if (!regCenter.isExisted(new JobNodePath(jobName).getServerNodePath(ip, username))) {
            return null;
        }
        JobBriefInfo result = new JobBriefInfo();
        result.setJobName(jobName);
        result.setStatus(getJobStatusByJobNameAndIp(jobName, ip, username));
        result.setInstanceCount(getJobInstanceCountByJobNameAndIp(jobName, ip, username));
        return result;
    }
    
    private JobStatus getJobStatusByJobNameAndIp(final String jobName, final String ip, String userName) {
        JobNodePath jobNodePath = new JobNodePath(jobName);
        String status = regCenter.get(jobNodePath.getServerNodePath(ip, userName));
        if ("DISABLED".equalsIgnoreCase(status)) {
            return JobStatus.DISABLED;
        } else {
            return JobStatus.OK;
        }
    }
    
    
    private int getJobInstanceCountByJobNameAndIp(final String jobName, final String ip, String userName) {
        int instanceCount = 0;
        JobNodePath jobNodePath = new JobNodePath(jobName);
        List<String> instances = regCenter.getChildrenKeys(jobNodePath.getInstancesNodePath(userName));
        for (String each : instances) {
            if (ip.equals(each.split("@-@")[0])) {
                instanceCount++;
            }
        }
        return instanceCount;
    }
}
