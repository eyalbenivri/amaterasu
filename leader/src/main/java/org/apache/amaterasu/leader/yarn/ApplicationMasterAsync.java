package org.apache.amaterasu.leader.yarn;

import org.apache.amaterasu.common.configuration.ClusterConfig;
import org.apache.amaterasu.common.dataobjects.ActionData;
import org.apache.amaterasu.leader.execution.JobLoader;
import org.apache.amaterasu.leader.execution.JobManager;
import org.apache.amaterasu.leader.utilities.DataLoader;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {

    private Configuration configuration;
    private NMClient nmClient;
    private JobManager jobManager;
    private CuratorFramework client;
    ClusterConfig config;

    public ApplicationMasterAsync(JobOpts opts) {
        //this.command = command;
        configuration = new YarnConfiguration();
        //this.numContainersToWaitFor = numContainersToWaitFor;
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
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
                System.out.println("[AM] Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
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

    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {

        JobOpts opts = ArgsParser.getJobOpts(args);

        ApplicationMasterAsync master = new ApplicationMasterAsync(opts);

        String propPath = System.getenv("PWD") + "/amaterasu.properties";
        FileInputStream propsFile = new FileInputStream(new File(propPath));

        master.config = ClusterConfig.apply(propsFile);
        master.initJob(opts);

        master.runMainLoop();

    }

    public void runMainLoop() throws Exception {

        AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();

        // Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");

        Thread.sleep(10000);
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

    private void initJob(JobOpts opts) throws InterruptedException {
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
        System.out.println("created jobManager");
        Thread.sleep(10000);
        jobManager.start();

        System.out.println("started jobManager");
        Thread.sleep(10000);
    }
}
