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

import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;
import com.dangdang.ddframe.job.lite.lifecycle.api.ShardingStatisticsAPI;
import com.dangdang.ddframe.job.lite.lifecycle.domain.ShardingInfo;
import com.dangdang.ddframe.job.lite.lifecycle.domain.ShardingInfo.ShardingStatus;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 作业分片状态展示的实现类.
 *
 * @author caohao
 */
@RequiredArgsConstructor
public final class ShardingStatisticsAPIImpl implements ShardingStatisticsAPI {
    
    private final CoordinatorRegistryCenter regCenter;
    
    @Override
    public Collection<ShardingInfo> getShardingInfo(final String jobName) {
        String shardingRootPath = new JobNodePath(jobName).getShardingNodePath();
        List<String> items = regCenter.getChildrenKeys(shardingRootPath);
        List<ShardingInfo> result = new ArrayList<>(items.size());
        for (String each : items) {
            result.add(getShardingInfo(jobName, each, ""));
        }
        Collections.sort(result);
        return result;
    }

    @Override
    public Collection<ShardingInfo> getShardingInfoByUserName(String jobName, String userName) {
        String shardingRootPath = new JobNodePath(jobName).getShardingNodePath(userName);
        List<String> items = regCenter.getChildrenKeys(shardingRootPath);
        List<ShardingInfo> result = new ArrayList<>(items.size());
        for (String each : items) {
            result.add(getShardingInfo(jobName, each, userName));
        }
        Collections.sort(result);
        return result;
    }

    private ShardingInfo getShardingInfo(final String jobName, final String item, final String userName) {
        ShardingInfo result = new ShardingInfo();
        result.setItem(Integer.parseInt(item));
        JobNodePath jobNodePath = new JobNodePath(jobName);
        String instanceId = regCenter.get(jobNodePath.getShardingNodePath(item, "instance", userName));
        boolean disabled = regCenter.isExisted(jobNodePath.getShardingNodePath(item, "disabled", userName));
        boolean running = regCenter.isExisted(jobNodePath.getShardingNodePath(item, "running", userName));
        boolean shardingError = !regCenter.isExisted(jobNodePath.getInstanceNodePath(instanceId, userName));
        result.setStatus(ShardingStatus.getShardingStatus(disabled, running, shardingError));
        result.setFailover(regCenter.isExisted(jobNodePath.getShardingNodePath(item, "failover", userName)));
        if (null != instanceId) {
            String[] ipAndPid = instanceId.split("@-@");
            result.setServerIp(ipAndPid[0]);
            result.setInstanceId(ipAndPid[1]);
        }
        return result;
    }
}
