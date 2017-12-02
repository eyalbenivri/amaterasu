/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.leader.yarn;

import org.apache.amaterasu.common.configuration.ClusterConfig;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Client {

    Configuration conf = new YarnConfiguration();
    ClusterConfig config = ClusterConfig(new FileInputStream("${arguments.home}/amaterasu.properties"))

    public void run(JobOpts opts) throws Exception {
//        final String comgmand = args[0];
//        final int n = Integer.valueOf(args[1]);
//        final Path jarPath = new Path(args[2]);

        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer =
                Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(
                Collections.singletonList(
                        "$JAVA_HOME/bin/java" +
                                " -Xmx256M" +
                                " org.apache.amaterasu.leader.yarn.ApplicationMaster job_1" + //TODO: from args
//                                " " + command +
//                                " " + String.valueOf(n) +
                                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                )
        );

        // Setup jar for ApplicationMaster
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupAppMasterJar(jarPath, appMasterJar);
        amContainer.setLocalResources(
                Collections.singletonMap("simpleapp.jar", appMasterJar));

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext =
                app.getApplicationSubmissionContext();
        appContext.setApplicationName("amaterasu-job"); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        System.out.println(
                "Application " + appId + " finished with" +
                        " state " + appState +
                        " at " + appReport.getFinishTime());

    }

    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim());
        }
        Apps.addToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*");
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client();

        CommandLineParser parser = new BasicParser();
        Options options = getOptions();
        CommandLine line = parser.parse( options, args );
        JobOpts opts = getJobOpts(line);

        c.run(opts);
    }

    private static Options getOptions() {

        Options options = new Options();
        options.addOption("r", "repo", true, "The git repo containing the job");
        options.addOption("b", "branch", true, "The branch to be executed (default is master)");
        options.addOption("e", "env", true, "The environment to be executed (test, prod, etc. values from the default env are taken if np env specified)");
        options.addOption("n", "name", true, "The name of the job");
        options.addOption("i", "job-id", true, "The jobId - should be passed only when resuming a job");
        options.addOption("r", "report", true, "The level of reporting");
        options.addOption("h", "home", true, "The level of reporting");

        return options;
    }

    private static JobOpts getJobOpts(CommandLine cli){

        JobOpts opts = new JobOpts();
        if (cli.hasOption("repo") ){
            opts.repo = cli.getOptionValue("repo");
        }

        if (cli.hasOption("branch") ){
            opts.branch = cli.getOptionValue("branch");
        }

        if (cli.hasOption("env") ){
            opts.env = cli.getOptionValue("env");
        }

        if (cli.hasOption("job-id") ){
            opts.jobId = cli.getOptionValue("job-id");
        }

        if (cli.hasOption("report") ){
            opts.report = cli.getOptionValue("report");
        }

        if (cli.hasOption("home") ){
            opts.home = cli.getOptionValue("home");
        }

        if (cli.hasOption("name") ){
            opts.name = cli.getOptionValue("name");
        }

        return opts;
    }
}

class JobOpts {
    public String repo = "";
    public String branch = "master";
    public String env = "default";
    public String name = "amaterasu-job";
    public String jobId = null;
    public String report ="code";
    public String home ="";
}