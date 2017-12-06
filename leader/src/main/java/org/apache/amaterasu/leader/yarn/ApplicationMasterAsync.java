package org.apache.amaterasu.leader.yarn;

import org.apache.amaterasu.common.configuration.ClusterConfig;
import org.apache.amaterasu.common.dataobjects.ActionData;
import org.apache.amaterasu.leader.execution.JobLoader;
import org.apache.amaterasu.leader.execution.JobManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationMasterAsync.class);
    private final FileSystem fs;
    private final YarnConfiguration yarnConfiguration;
    private final NMClientAsyncImpl nmClient;
    private final JobManager jobManager;
    private final CuratorFramework client;
    private final ClusterConfig config;
    private final AMRMClientAsync<AMRMClient.ContainerRequest> rmClient;

    public ApplicationMasterAsync(JobOpts opts) throws IOException {
        yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        yarnConfiguration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        yarnConfiguration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        yarnConfiguration.set("fs.file.impl", LocalFileSystem.class.getName());
        //this.numContainersToWaitFor = numContainersToWaitFor;
        String propPath = System.getenv("PWD") + "/amaterasu.properties";
        LOGGER.info("Reading properties file {}", propPath);
        FileInputStream propsFile = new FileInputStream(new File(propPath));

        this.config = ClusterConfig.apply(propsFile);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(config.zk(), retryPolicy);
        client.start();

        if (opts.jobId != null && !opts.jobId.isEmpty()) {
            jobManager = JobLoader.reloadJob(
                    opts.jobId,
                    client,
                    config.Jobs().Tasks().attempts(),
                    new LinkedBlockingQueue<ActionData>()
            );
        } else {
            jobManager = JobLoader.loadJob(
                    opts.repo,
                    opts.branch,
                    opts.newJobId,
                    client,
                    config.Jobs().Tasks().attempts(),
                    new LinkedBlockingQueue<ActionData>()
            );
        }
        LOGGER.info("created jobManager");
        jobManager.start();
        LOGGER.info("started jobManager");
        nmClient = new NMClientAsyncImpl(new YarnNMCallbackHandler());
        nmClient.init(yarnConfiguration);
        nmClient.start();

        rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(this.yarnConfiguration);
        rmClient.start();

        fs = FileSystem.get(this.yarnConfiguration);
    }

    private void runMainLoop() throws Exception {

        Path jarPath = new Path(config.YARN().hdfsJarsPath());
        Path jarPathQualified = fs.makeQualified(jarPath);

        // Register with ResourceManager
        String appMasterHostname = NetUtils.getHostname();
        LOGGER.info("Registering application with Resource Manager");
        RegisterApplicationMasterResponse registerApplicationMasterResponse = rmClient.registerApplicationMaster(appMasterHostname, 3000, "");
        LOGGER.info("Registered application with Resource Manager");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        //TODO: Bring from action config
        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        ActionData actionData = jobManager.getNextActionData();
//        for (int i = 0; i < numContainersToWaitFor; ++i) {
        if (actionData != null) {
            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
            System.out.println("[AM] Making res-req " + actionData.id());
            rmClient.addContainerRequest(containerAsk);
        }

        System.out.println("[AM] waiting for containers to finish");
//        while (!doneWithContainers()) {
//            Thread.sleep(100);
//        }

        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }

    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);

//                ActionData actionData = jobManager.getNextActionData();
//                String taskData = DataLoader.getTaskData(actionData, env).toString();
//                val execData = DataLoader.getExecutorData(env)

                ctx.setCommands(
                        Collections.singletonList(
                                "echo Working" +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));
                LOGGER.info("Launching container {}", container.getId());
                nmClient.startContainerAsync(container, ctx);
            } catch (Exception ex) {
                LOGGER.error("Error launching container {}", container.getId(), ex);
            }
        }
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            synchronized (this) {
                //numContainersToWaitFor--;
            }
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onReboot() {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        return 0;
    }

    public static void main(String[] args) throws Exception {
        JobOpts opts = ArgsParser.getJobOpts(args);
        ApplicationMasterAsync master = new ApplicationMasterAsync(opts);
        master.runMainLoop();
    }
}
