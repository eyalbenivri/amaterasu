package org.apache.amaterasu.sdk;

import org.apache.amaterasu.common.dataobjects.ExecData;
import org.apache.amaterasu.common.execution.actions.Notifier;

import java.io.ByteArrayOutputStream;

/**
 * RunnersProvider an interface representing a factory that creates a group of related
 * runners. For example the SparkProvider is a factory for all the spark runners
 * (Scala, Python, R, SQL, etc.)
 */
public interface RunnersProvider {

    void init(ExecData data,
              String jobId,
              ByteArrayOutputStream outStream,
              Notifier notifier,
              String executorId);
    String getGroupIdentifier();
    AmaterasuRunner getRunner(String id);
}