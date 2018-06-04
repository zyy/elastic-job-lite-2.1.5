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

package com.dangdang.ddframe.job.lite.lifecycle.internal.operate;

import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;
import com.dangdang.ddframe.job.lite.lifecycle.api.JobOperateAPI;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.util.List;

/**
 * 操作作业的实现类.
 *
 * @author caohao
 */
public final class JobOperateAPIImpl implements JobOperateAPI {
    
    private final CoordinatorRegistryCenter regCenter;
    
    public JobOperateAPIImpl(final CoordinatorRegistryCenter regCenter) {
        this.regCenter = regCenter;
    }
    
    @Override
    public void trigger(final Optional<String> jobName, final Optional<String> serverIp) {
        if (jobName.isPresent()) {
            JobNodePath jobNodePath = new JobNodePath(jobName.get());
            for (String each : regCenter.getChildrenKeys(jobNodePath.getInstancesNodePath())) {
                regCenter.persist(jobNodePath.getInstanceNodePath(each), "TRIGGER");
            }
        }
    }

    @Override
    public void trigger(Optional<String> jobName, Optional<String> serverIp, String userName) {
        if (jobName.isPresent()) {
            JobNodePath jobNodePath = new JobNodePath(jobName.get());
            for (String each : regCenter.getChildrenKeys(jobNodePath.getInstancesNodePath(userName))) {
                regCenter.persist(jobNodePath.getInstanceNodePath(each, userName), "TRIGGER");
            }
        }
    }

    @Override
    public void disable(final Optional<String> jobName, final Optional<String> serverIp) {
        disableOrEnableJobs(jobName, serverIp, true, "");
    }

    @Override
    public void disable(Optional<String> jobName, Optional<String> serverIp, String userName) {
        disableOrEnableJobs(jobName, serverIp, true, userName);
    }

    @Override
    public void enable(final Optional<String> jobName, final Optional<String> serverIp) {
        disableOrEnableJobs(jobName, serverIp, false, "");
    }

    @Override
    public void enable(final Optional<String> jobName, final Optional<String> serverIp, String userName) {
        disableOrEnableJobs(jobName, serverIp, false, userName);
    }

    private void disableOrEnableJobs(final Optional<String> jobName, final Optional<String> serverIp, final boolean disabled, String userName) {
        Preconditions.checkArgument(jobName.isPresent() || serverIp.isPresent(), "At least indicate jobName or serverIp.");
        if (jobName.isPresent() && serverIp.isPresent()) {
            persistDisabledOrEnabledJob(jobName.get(), serverIp.get(), disabled, userName);
        } else if (jobName.isPresent()) {
            JobNodePath jobNodePath = new JobNodePath(jobName.get());
            for (String each : regCenter.getChildrenKeys(jobNodePath.getServerNodePathByUserName(userName))) {
                if (disabled) {
                    regCenter.persist(jobNodePath.getServerNodePath(each, userName), "DISABLED");
                } else {
                    regCenter.persist(jobNodePath.getServerNodePath(each, userName), "");
                }
            }
        } else if (serverIp.isPresent()) {
            List<String> jobNames = regCenter.getChildrenKeys("/" + userName);
            for (String each : jobNames) {
                if (regCenter.isExisted(new JobNodePath(each).getServerNodePath(serverIp.get(), userName))) {
                    persistDisabledOrEnabledJob(each, serverIp.get(), disabled, userName);
                }
            }
        }
    }
    
    private void persistDisabledOrEnabledJob(final String jobName, final String serverIp, final boolean disabled, final String userName) {
        JobNodePath jobNodePath = new JobNodePath(jobName);
        String serverNodePath = jobNodePath.getServerNodePath(serverIp, userName);
        if (disabled) {
            regCenter.persist(serverNodePath, "DISABLED");
        } else {
            regCenter.persist(serverNodePath, "");
        }
    }

    @Override
    public void shutdown(Optional<String> jobName, Optional<String> serverIp) {
        this.shutdown(jobName, serverIp, "");
    }

    @Override
    public void shutdown(final Optional<String> jobName, final Optional<String> serverIp, String userName) {
        Preconditions.checkArgument(jobName.isPresent() || serverIp.isPresent(), "At least indicate jobName or serverIp.");
        if (jobName.isPresent() && serverIp.isPresent()) {
            JobNodePath jobNodePath = new JobNodePath(jobName.get());
            for (String each : regCenter.getChildrenKeys(jobNodePath.getInstancesNodePath(userName))) {
                if (serverIp.get().equals(each.split("@-@")[0])) {
                    regCenter.remove(jobNodePath.getInstanceNodePath(each, userName));
                }
            }
        } else if (jobName.isPresent()) {
            JobNodePath jobNodePath = new JobNodePath(jobName.get());
            for (String each : regCenter.getChildrenKeys(jobNodePath.getInstancesNodePath(userName))) {
                regCenter.remove(jobNodePath.getInstanceNodePath(each, userName));
            }
        } else if (serverIp.isPresent()) {
            List<String> jobNames = regCenter.getChildrenKeys("/" + userName);
            for (String job : jobNames) {
                JobNodePath jobNodePath = new JobNodePath(job);
                List<String> instances = regCenter.getChildrenKeys(jobNodePath.getInstancesNodePath(userName));
                for (String each : instances) {
                    if (serverIp.get().equals(each.split("@-@")[0])) {
                        regCenter.remove(jobNodePath.getInstanceNodePath(each, userName));
                    }
                }
            }
        }
    }

    @Override
    public void remove(Optional<String> jobName, Optional<String> serverIp) {
        this.remove(jobName, serverIp, "");
    }

    @Override
    public void remove(final Optional<String> jobName, final Optional<String> serverIp, String userName) {
        shutdown(jobName, serverIp, userName);
        if (jobName.isPresent() && serverIp.isPresent()) {
            regCenter.remove(new JobNodePath(jobName.get()).getServerNodePath(serverIp.get(), userName));
        } else if (jobName.isPresent()) {
            JobNodePath jobNodePath = new JobNodePath(jobName.get());
            List<String> servers = regCenter.getChildrenKeys(jobNodePath.getServerNodePathByUserName(userName));
            for (String each : servers) {
                regCenter.remove(jobNodePath.getServerNodePath(each, userName));
            }
        } else if (serverIp.isPresent()) {
            List<String> jobNames = regCenter.getChildrenKeys("/" + userName);
            for (String each : jobNames) {
                regCenter.remove(new JobNodePath(each).getServerNodePath(serverIp.get(), userName));
            }
        }
    }
}
