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
import org.apache.commons.cli.*;

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


    public void run(JobOpts opts, String[] args) throws Exception {

        ClusterConfig config = new ClusterConfig();
        config.load(new FileInputStream(opts.home + "/amaterasu.properties"));
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

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

        String newId = "";
        if(opts.jobId == null){

            newId = "--new-job-id " + appContext.getApplicationId().toString();
        }

        ContainerLaunchContext amContainer =
                Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(
                Collections.singletonList(
                        "$JAVA_HOME/bin/java" +
                                " -Xmx256M" +
                                " org.apache.amaterasu.leader.yarn.ApplicationMasterAsync " + //TODO: from args
                                joinStrings(args) +
                                newId +
                                "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout " +
                                "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                )
        );

        String p = config.YARN().hdfsJarsPath();
        Path jarPath = new Path(p);

        // get version of build
        String version = config.version();

        // get local resources pointers that will be set on the master container env
        String leaderJarPath = "/bin/leader-" + version + "-all.jar";
        Path mergedPath = Path.mergePaths(jarPath, new Path(leaderJarPath));

        // Setup jar for ApplicationMaster
        LocalResource appMasterJar = setupLocalResource(mergedPath);
        LocalResource propFile = setupLocalResource(Path.mergePaths(jarPath, new Path("/amaterasu.properties")));

        System.out.println("===> " + appMasterJar);

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        localResources.put("simpleapp.jar", appMasterJar);
        localResources.put("amaterasu.properties", propFile);

        amContainer.setLocalResources(localResources);

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
//        ApplicationSubmissionContext appContext =
//                app.getApplicationSubmissionContext();
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

    private LocalResource setupLocalResource(Path jarPath) throws IOException {

        FileStatus stat = FileSystem.get(conf).getFileStatus(jarPath);
        LocalResource resurce = Records.newRecord(LocalResource.class);
        resurce.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        resurce.setSize(stat.getLen());
        resurce.setTimestamp(stat.getModificationTime());
        resurce.setType(LocalResourceType.FILE);
        resurce.setVisibility(LocalResourceVisibility.PUBLIC);

        return resurce;
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

        JobOpts opts = ArgsParser.getJobOpts(args);

        c.run(opts, args);
    }

    private static String joinStrings(String[] str){

        StringBuilder builder = new StringBuilder();
        for(String s : str) {
            builder.append(s);
            builder.append(" ");
        }
        return builder.toString();

    }

}

