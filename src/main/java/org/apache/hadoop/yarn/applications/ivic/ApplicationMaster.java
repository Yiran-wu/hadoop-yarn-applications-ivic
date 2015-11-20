/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.applications.ivic;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Tables;

/**
 * An ApplicationMaster for executing shell commands on a set of launched
 * containers using the YARN framework.
 * 
 * <p>
 * This class is meant to act as an example on how to write yarn-based
 * application masters.
 * </p>
 * 
 * <p>
 * The ApplicationMaster is started on a container by the
 * <code>ResourceManager</code>'s launcher. The first thing that the
 * <code>ApplicationMaster</code> needs to do is to connect and register itself
 * with the <code>ResourceManager</code>. The registration sets up information
 * within the <code>ResourceManager</code> regarding what host:port the
 * ApplicationMaster is listening on to provide any form of functionality to a
 * client as well as a tracking URL that a client can use to keep track of
 * status/job history if needed. However, in the ivic, tracking URL
 * and appMasterHost:appMasterRpcPort are not supported.
 * </p>
 *
 * AppMaster将自己所在的ip:port注册在RM中，然后提供给client一个tracking url,以方便client追踪job的status和history
 * 但是在distirbutedshell中，tracking url和appMasterHost:appMasterRpcPort并不支持，需要自己实现
 * TODO - 在ivic5.0中，将AppMaster启动后，自动将ip:port反馈给portal对应的user,并保存在DB中，以便用户后续访问
 *
 * <p>
 * The <code>ApplicationMaster</code> needs to send a heartbeat to the
 * <code>ResourceManager</code> at regular intervals to inform the
 * <code>ResourceManager</code> that it is up and alive. The
 * {@link ApplicationMasterProtocol#allocate} to the <code>ResourceManager</code> from the
 * <code>ApplicationMaster</code> acts as a heartbeat.
 *
 * AM定时向RM发送心跳信息，ApplicationMasterProtocol#allocate方法心跳执行
 *
 * <p>
 * For the actual handling of the job, the <code>ApplicationMaster</code> has to
 * request the <code>ResourceManager</code> via {@link AllocateRequest} for the
 * required no. of containers using {@link ResourceRequest} with the necessary
 * resource specifications such as node location, computational
 * (memory/disk/cpu) resource requirements. The <code>ResourceManager</code>
 * responds with an {@link AllocateResponse} that informs the
 * <code>ApplicationMaster</code> of the set of newly allocated containers,
 * completed containers as well as current state of available resources.
 * </p>
 * 
 * <p>
 * For each allocated container, the <code>ApplicationMaster</code> can then set
 * up the necessary launch context via {@link ContainerLaunchContext} to specify
 * the allocated container id, local resources required by the executable, the
 * environment to be setup for the executable, commands to execute, etc. and
 * submit a {@link StartContainerRequest} to the {@link ContainerManagementProtocol} to
 * launch and execute the defined commands on the given allocated container.
 * </p>
 * 
 * 对于分配的container,AM都建立ContainerLaunchContext对象（id/loacal resource等），
 * 然后通过ContainerManagementProtocol提交StartContainerRequest，在container中执行命令
 * 
 * <p>
 * The <code>ApplicationMaster</code> can monitor the launched container by
 * either querying the <code>ResourceManager</code> using
 * {@link ApplicationMasterProtocol#allocate} to get updates on completed containers or via
 * the {@link ContainerManagementProtocol} by querying for the status of the allocated
 * container's {@link ContainerId}.
 *
 * AM通过两种方式monitor container:(1)使用ApplicationMasterProtocol#allocate查询RM，得到completed container
 * (2)通过ContainerManagementProtocol根据分配的container's id查询状态
 *
 * <p>
 * After the job has been completed, the <code>ApplicationMaster</code> has to
 * send a {@link FinishApplicationMasterRequest} to the
 * <code>ResourceManager</code> to inform it that the
 * <code>ApplicationMaster</code> has been completed.
 * 
 * 执行完成后，AM还要通知RM
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  // 开头的DS我理解为“Distributed shell”
  @VisibleForTesting
  @Private
  public static enum DSEvent {
    DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
  }
  
  // 两个事件实体：appMaster和container
  @VisibleForTesting
  @Private
  public static enum DSEntity {
    DS_APP_ATTEMPT, DS_CONTAINER
  }

  // Configuration
  private Configuration conf;

  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;

  // In both secure and non-secure modes, this points to the job-submitter.
  private UserGroupInformation appSubmitterUgi;

  // Handle to communicate with the Node Manager
  private NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  private NMCallbackHandler containerListener;
  // Application Attempt Id ( combination of attemptId and fail count )
  @VisibleForTesting
  protected ApplicationAttemptId appAttemptID;

  // TODO
  // For status update for clients - yet to be implemented
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";

  // No. of containers to run shell command on
  @VisibleForTesting
  protected int numTotalContainers = 1;
  // Memory to request for the container on which the shell command will run
  private int containerMemory = 10;
  // VirtualCores to request for the container on which the shell command will run
  private int containerVirtualCores = 1;
  // Priority of the request
  private int requestPriority;
  
  // add by kongs. 改为全局变量，并赋一个初始值
  private int maxMem = 512;
  private int maxVCores = 1;

  /**
   * numCompletedContainers:已经完成的container数量（包括成功和失败）
   * numAllocatedContainers:RM已经分配的container
   * numFailedContainers:失败的container，是numCompletedContainers的一个子集
   * numRequestedContainers:AM已经请求的container的数量（container只能请求一次，除非原始的request信息变更！）
   * 所以：
   * numRequestedContainers - numAllocatedContainers = 已经申请但是还未分配的container数量
   * numCompletedContainers - numFailedContainers = 执行成功的container数量
   * 
   */
  // Counter for completed containers ( complete denotes successful or failed )
  private AtomicInteger numCompletedContainers = new AtomicInteger();
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  @VisibleForTesting
  protected AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of failed containers
  private AtomicInteger numFailedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again.
  // Only request for more if the original requirement changes.
  @VisibleForTesting
  protected AtomicInteger numRequestedContainers = new AtomicInteger();
  
  // Shell command to be executed
  private String shellCommand = "";
  // Args to be passed to the shell command
  private String shellArgs = "";
  // Env variables to be setup for the shell command
  private Map<String, String> shellEnv = new HashMap<String, String>();
  
  // Location of shell script ( obtained from info set in env )
  // Shell script path in fs
  private String scriptPath = "";
  // Timestamp needed for creating a local resource
  private long shellScriptPathTimestamp = 0;
  // File length needed for local resource
  private long shellScriptPathLen = 0;

  // 简单说就是记录历史信息的
  // Timeline domain ID
  private String domainId = null;

  // Hardcoded path to shell script in launch container's local env
  // Hardcoded path：硬编码，应该指静态变量
  // 直接通过Client类调用静态final变量
  private static final String ExecShellStringPath = Client.SCRIPT_PATH + ".sh";
  private static final String ExecBatScripStringtPath = Client.SCRIPT_PATH
      + ".bat";

  // Hardcoded path to custom log_properties
  private static final String log4jPath = "log4j.properties";

  private static final String shellCommandPath = "shellCommands";
  private static final String shellArgsPath = "shellArgs";
  
  // ivic job在hdfs上的路径
  //private static final String ivicJobPath = "ivicJob";

  // AM是否执行完成
  private volatile boolean done;

  // TODO 令牌，咱没有搞清具体流程
  private ByteBuffer allTokens;

  // Launch threads
  // 保存要执行container的线程
  private List<Thread> launchThreads = new ArrayList<Thread>();

  // Timeline Client
  // 和Timeline Server通讯，记录history
  private TimelineClient timelineClient;

  private final String linux_bash_command = "bash";
  private final String windows_command = "cmd /c";
  
  //ivic portal user id
  private static String userID = null;

  // get table name from task and job's target_object_type
  // TODO 这里暂时只用到vm
  private Map<String, String> tables = new HashMap<String, String>();
  // operationToState is for update the tasks' object during operations
  private Map<String, String> operationToState = new HashMap<String, String>();
  // FinishedTransDict is for update the status of successfully finished jobs
 
  // 根据虚拟机操作匹配后台方法
  private Map<String, String> vmMethodMapping = new HashMap<String, String>();
 
  // 保存task的队列
  // BlockingQueue是一个阻塞队列，可以自动进行同步，LinkedBlockingQueue默认大小为Integer.MAX_VALUE，也可手动指定大小
  BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<Task>();
  
  // 暂存处理中的task
  // 此时AM已经为该task发送了资源请求，但是后续还没有执行；等得到container并执行完以后，根据task的内容，去更改相应对象的状态
  // 多线程情况下，task和container都是FIFO的模式，所以可以保证一一对应
  static BlockingQueue<Task> pendingTaskQueue = new LinkedBlockingQueue<Task>();
  
  // 保存vm和container对应关系，以便在container执行后更改vm的状态
  // 目前task的类型只有vm和vdisk，后续的网络和磁盘单独考虑；而在vm和vdisk中，只有vm有状态
  static ConcurrentHashMap<String, ContainerId> vmToContainer= new ConcurrentHashMap<String, ContainerId>();
  
  // 创建数据库连接，一个appMaster对应一个数据库连接
  // TODO 后续可以加入portal与AM的RPC通讯
  ConnectDataBase con = new ConnectDataBase();
  
  /**
   * @param args Command line args
   */
  public static void main(String[] args) {
    // TODO 这里将result注释掉，并且不退出，应该可以实现AM的long-running
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      /*
       * Date: 2015/11/19
       * 如果AM一直运行，则result永远得不到结果，除非因为异常退出
       */
      result = appMaster.finish();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      LogManager.shutdown();
      ExitUtil.terminate(1, t);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging
   */
  private void dumpOutDebugInfo() {

    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val="
          + env.getValue());
    }

    BufferedReader buf = null;
    try {
      String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") :
        Shell.execCommand("ls", "-al");
      buf = new BufferedReader(new StringReader(lines));
      String line = "";
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.cleanup(LOG, buf);
    }
  }

  public ApplicationMaster() {
    // Set up the configuration
    conf = new YarnConfiguration();
  }

  /**
   * Parse command line options
   *
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  public boolean init(String[] args) throws ParseException, IOException {
    Options opts = new Options();
    opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
    opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command");
    opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("debug", false, "Dump out debug information");
    // 用户id和AppMaster一一对应
    opts.addOption("user_id", true, "User id which the current AppMaster belongs to.");
    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException(
          "No args specified for application master to initialize");
    }

    //Check whether customer log4j.properties file exists
    if (fileExist(log4jPath)) {
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class,
            log4jPath);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }
    
    userID = cliParser.getOptionValue("user_id");
    LOG.info("【470】Get user id from client: " + userID);

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption("debug")) {
      dumpOutDebugInfo();
    }

    Map<String, String> envs = System.getenv();

    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        throw new IllegalArgumentException(
            "Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs
          .get(Environment.CONTAINER_ID.name()));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HOST.name())) {
      throw new RuntimeException(Environment.NM_HOST.name()
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(Environment.NM_HTTP_PORT
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_PORT.name())) {
      throw new RuntimeException(Environment.NM_PORT.name()
          + " not set in the environment");
    }

    LOG.info("Application master for app" + ", appId="
        + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
        + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());
    
    if (!fileExist(shellCommandPath)
        && envs.get(DSConstants.IVICSCRIPTLOCATION).isEmpty()) {
      throw new IllegalArgumentException(
          "No shell command or shell script specified to be executed by application master");
    }

    if (fileExist(shellCommandPath)) { 
      shellCommand = readContent(shellCommandPath);
    }

    if (fileExist(shellArgsPath)) {
      shellArgs = readContent(shellArgsPath);
    }

    if (cliParser.hasOption("shell_env")) {
      String shellEnvs[] = cliParser.getOptionValues("shell_env");
      for (String env : shellEnvs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          shellEnv.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length() - 1)) {
          val = env.substring(index + 1);
        }
        shellEnv.put(key, val);
      }
    }

    if (envs.containsKey(DSConstants.IVICSCRIPTLOCATION)) {
      scriptPath = envs.get(DSConstants.IVICSCRIPTLOCATION);

      if (envs.containsKey(DSConstants.IVICSCRIPTTIMESTAMP)) {
        shellScriptPathTimestamp = Long.valueOf(envs
            .get(DSConstants.IVICSCRIPTTIMESTAMP));
      }
      if (envs.containsKey(DSConstants.IVICSCRIPTLEN)) {
        shellScriptPathLen = Long.valueOf(envs
            .get(DSConstants.IVICSCRIPTLEN));
      }
      if (!scriptPath.isEmpty()
          && (shellScriptPathTimestamp <= 0 || shellScriptPathLen <= 0)) {
        LOG.error("Illegal values in env for shell script path" + ", path="
            + scriptPath + ", len=" + shellScriptPathLen + ", timestamp="
            + shellScriptPathTimestamp);
        throw new IllegalArgumentException(
            "Illegal values in env for shell script path");
      }
    }

    if (envs.containsKey(DSConstants.IVICTIMELINEDOMAIN)) {
      domainId = envs.get(DSConstants.IVICTIMELINEDOMAIN);
    }

    // 以下三项如果为空，则采用默认值
    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
    containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
    
    
    numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
    // modify by kongs. 11-19
    // 现在即使没有container的数量可以为0，继续等待下一个请求
    /*
    if (numTotalContainers == 0) {
      throw new IllegalArgumentException(
          "Cannot run ivic with no containers");
    }*/
    
    requestPriority = Integer.parseInt(cliParser
        .getOptionValue("priority", "0"));

    // 数据库表和task操作对象的对应关系
    tables.put("VirtualMachine", "virtual_machines");
    tables.put("Vdisk", "vdisks");
    tables.put("Vnet", "vnets");
    LOG.info("*********db tables:" + tables.toString());
    
    // task的操作状态的变更
    operationToState.put("deploy", "deploying");
    operationToState.put("start", "starting");
    operationToState.put("stop", "stopping");
    operationToState.put("undeploy", "undeploying");
    operationToState.put("hibernate", "hibernating");
    operationToState.put("resume", "resuming");
    operationToState.put("reboot", "rebooting");
    operationToState.put("edit", "editing");
    LOG.info("*********operationToState:" + operationToState.toString());
    
    // VM的操作方法
    vmMethodMapping.put("deploy", "deployVM");
    vmMethodMapping.put("start", "startVM");
    vmMethodMapping.put("stop", "stopVM");
    vmMethodMapping.put("undeploy", "undeployVM");
    vmMethodMapping.put("edit", "deployVMInfo");
    LOG.info("*********vmMethodMapping:" + vmMethodMapping.toString());
    
    // Creating the Timeline Client
    timelineClient = TimelineClient.createTimelineClient();
    timelineClient.init(conf);
    timelineClient.start();

    return true;
  }

  /**
   * Helper function to print usage
   *
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  /**
   * Main run function for the application master
   *
   * @throws YarnException
   * @throws IOException
   * @throws SQLException 
   */
  @SuppressWarnings({ "unchecked" })
  public void run() throws YarnException, IOException, SQLException {
    LOG.info("Starting ApplicationMaster");

    // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
    // are marked as LimitedPrivate
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    LOG.info("Executing with tokens:");
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info(token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Create appSubmitterUgi and add original tokens to it
    String appSubmitterUserName =
        System.getenv(ApplicationConstants.Environment.USER.name());
    appSubmitterUgi =
        UserGroupInformation.createRemoteUser(appSubmitterUserName);
    appSubmitterUgi.addCredentials(credentials);

    // 发布AppMaster启动的事件
    publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
        DSEvent.DS_APP_ATTEMPT_START, domainId, appSubmitterUgi);

    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener); // 心跳周期1000ms
    amRMClient.init(conf);
    amRMClient.start();//主要更改状态

    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    // 主要用来接收状态请求
    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // 可以参考MapReduce，该框架实现了client直接和AM通讯的机制！！
    
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master
    // client用AppMaster在RM中注册的rpc port，向AppMaster通讯

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    // TODO 这里的appMasterRpcPort端口信息直接采用默认值-1，appMasterTrackingUrl也为空串
    appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response = amRMClient
        .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
            appMasterTrackingUrl);
    
    // Dump out information about cluster capability as seen by the
    // resource manager
    maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    
    maxVCores = response.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);
    
    // AppMaster循环访问数据库，while(true)
    // ConnectDataBase con = new ConnectDataBase();
    
    String sql = "update users set app_master_hostname = '" + appMasterHostname + "' where id = " + userID;
    con.executeUpdate(sql);
    LOG.info("******update appMaster hostname in portal database: " + sql);
    
    // 暂时创建一个读线程
    // TODO 问题：在初始化这个线程时，传入的taskQueue是空的，那以后如果通过其他线程往里添加东西，是更新之后的taskQueue呢？还是初始化时的taskQueue呢？
    
    /*
    LOG.info("创建生产者进程！");
    Runnable jobReader = new LoopJobReader();
    Thread producer = new Thread(jobReader);
    producer.start();
    
    LOG.info("创建消费者进程！");
    Runnable taskRunner = new TaskRunner();
    Thread consumer = new Thread(taskRunner);
    consumer.start();
    */
    
    ExecutorService service = Executors.newCachedThreadPool();
    // 轮询数据库的线程
    LoopJobReader producer = new LoopJobReader();
    // 读取job并解析执行任务的线程
    TaskRunner consumer = new TaskRunner();
    LOG.info("创建生产者进程！");
    service.submit(producer);
    LOG.info("创建消费者进程！");
    service.submit(consumer);
    
    //doLoop(con);
  }
  
  @VisibleForTesting
  NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler(this);
  }

  @VisibleForTesting
  protected boolean finish() {
    /**
     * Date:2015/11/19
     * wait for completion.
     * 轮询检查AM最终是否完成；
     * 现在没有numTotalContainers(一开始预设的要执行container的数量，现在可以为0)的概念，只有numCompletedContainers，
     * 所以，在退出AM时，只根据done的值退出
     */
     
    //while (!done && (numCompletedContainers.get() != numTotalContainers)) {
    while (!done) {
      try {
        // 原来值为200，现在AM为long-running，所以没必要那么频繁检查
        Thread.sleep(2000);
      } catch (InterruptedException ex) {}
    }

    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      } catch (InterruptedException e) {
        LOG.info("Exception thrown in thread join: " + e.getMessage());
        e.printStackTrace();
      }
    }

    // When the application completes, it should stop all running containers
    // TODO 在AM退出时，running的container不应该收到影响
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");

    /**
     * Date: 2015/11/19
     * 解释：这里的AM退出分为两种情况：正常（container全部执行完）和异常（container没有执行完而因为其他原因中断）
     * 现在：只有异常退出，所以把success直接置为false
     */
    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success = true;
    /*if (numFailedContainers.get() == 0 && 
        numCompletedContainers.get() == numTotalContainers) { //执行成功
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else { //执行失败
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", total=" + numTotalContainers
          + ", completed=" + numCompletedContainers.get() + ", allocated="
          + numAllocatedContainers.get() + ", failed="
          + numFailedContainers.get();
      LOG.info(appMessage);
      success = false;
    }*/
    appStatus = FinalApplicationStatus.FAILED;
    appMessage = "Diagnostics." + ", total=" + numTotalContainers
        + ", completed=" + numCompletedContainers.get() + ", allocated="
        + numAllocatedContainers.get() + ", failed="
        + numFailedContainers.get();
    LOG.info(appMessage);
    success = false;
    try { //无论成功与否，都将AM在RM的注册信息删除掉
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }
    
    // AM与RM通讯的句柄关闭
    amRMClient.stop();

    return success;
  }
  
  // 回调类，被RM调用通知AM
  // 该类中每个方法的含义，查看<code>AMRMClientAsync<code>中的<code>CallbackHandler<code>接口
  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    /**
     * 检查已完成的 Container 的数量是否达到了需求，没有的话，继续添加需求。
     * 调用时机：RM的心跳应答中包含完成的container信息
     * TODO 现在的iVIC中，默认一个job对应一个task对应一个container，后续如果加入其他的网络等信息则需要再生成其他task
     */
    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Got response from RM for container ask, completedCnt="
          + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info(appAttemptID + " got container status for containerID="
            + containerStatus.getContainerId() + ", state="
            + containerStatus.getState() + ", exitStatus="
            + containerStatus.getExitStatus() + ", diagnostics="
            + containerStatus.getDiagnostics());

        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);

        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          // container failed
          if (ContainerExitStatus.ABORTED != exitStatus) {
            // shell script failed
            // counts as completed
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
          } else {
            // container was killed by framework, possibly preempted
            // we should re-try as the container was lost for some reason
            numAllocatedContainers.decrementAndGet();
            numRequestedContainers.decrementAndGet();
            // we do not need to release the container as it would be done
            // by the RM
          }
        } else {
          // nothing to do
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          LOG.info("Container completed successfully." + ", containerId="
              + containerStatus.getContainerId());
        }
        publishContainerEndEvent(
            timelineClient, containerStatus, domainId, appSubmitterUgi);
      }
      
      /**
       * 原来：如果container执行失败，重新执行
       * 现在：即使container失败也不重新执行，因为这涉及到container的资源量的大小，而每次申请的资源不一定一样
       */
      // ask for more containers if any failed
      /*int askCount = numTotalContainers - numRequestedContainers.get();
      numRequestedContainers.addAndGet(askCount);
      if (askCount > 0) {
        for (int i = 0; i < askCount; ++i) {
          ContainerRequest containerAsk = setupContainerAskForRM();
          amRMClient.addContainerRequest(containerAsk);
        }
      }*/
      
      /**
       * Date：11-19
       * 原来：完成的container数量等于一开始要请求的container数量，说明执行结束，done=true
       * 现在：AM为long-running，一开始要请求的container数量可以为0，根据是否有job生成来完成任务执行
       */
      // 完成的container数量等于一开始要请求的container数量，则done=true，表示完成！
      /*if (numCompletedContainers.get() == numTotalContainers) {
        done = true;
      }*/
    }

    /**
     * 这是被RM调用的回调类中的回调方法，RM分配了新的container，就调用该方法，开始让AM与NM通信
     * 获得新申请的 Container ，创建一个新线程，设置 ContainerLaunchContext，
     * 最终调用 NMClientAsync.startContainerAsync() 来启动 Container.
     * 调用时机：RM为AM返回的心跳应答中包含新分配的container
     * 若心跳应答中同时包含完成的container和新分配的container，则该回调函数在onContainersCompleted之后执行
     * 
     * 注意：这里是AM和NM开始交互的地方！即，创建新线程，设置container和NMCallbackHandler句柄信息，然后去启动container
     * 
     * 在iVIC中，一次暂时只申请一个container，所以即使是list保存container，仍然只有一个
     */
    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info("Got response from RM for container ask, allocatedCnt="
          + allocatedContainers.size());
      //修改已分配container的数量
      numAllocatedContainers.addAndGet(allocatedContainers.size());
      for (Container allocatedContainer : allocatedContainers) {
        LOG.info("Launching shell command on a new container."
            + ", containerId=" + allocatedContainer.getId()
            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
            + ":" + allocatedContainer.getNodeId().getPort()
            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
            + ", containerResourceMemory="
            + allocatedContainer.getResource().getMemory()
            + ", containerResourceVirtualCores="
            + allocatedContainer.getResource().getVirtualCores()
            + ", containerToken"
            +allocatedContainer.getContainerToken().getIdentifier().toString());

        // 将vm与为其申请的container的对应关系保存在HashMap中，以便在container执行后用来更改vm的状态
        if (!pendingTaskQueue.isEmpty()) {
            Task task = pendingTaskQueue.peek();
            
            if (task.getTargetObjectType().equals("VirtualMachine")) {
                vmToContainer.put(task.getTargetObjectId(), allocatedContainer.getId());
            }
            
            LaunchContainerRunnable runnableLaunchContainer =
                new LaunchContainerRunnable(allocatedContainer, containerListener);
            Thread launchThread = new Thread(runnableLaunchContainer);
    
            // launch and start the container on a separate thread to keep
            // the main thread unblocked
            // as all containers may not be allocated at one go.
            launchThreads.add(launchThread);
            launchThread.start();
        }
        else {
            amRMClient.releaseAssignedContainer(allocatedContainer.getId());
        }
      }
    }

    /**
     * Date:2015/11/19
     * TODO 此处暂时保留，因为AM不完全因为此条件，还有container数量的限制
     * 在<code>AMRMClientAsyncImpl</code>中的<code>HeartbeatThread</code>中出错时才调用
     * Called when the ResourceManager wants the ApplicationMaster to shutdown
     * for being out of sync etc. The ApplicationMaster should not unregister
     * with the RM unless the ApplicationMaster wants to be the last attempt.
     */
    // RM发送shutdown的指令，设置done=true，说明执行结束
    @Override
    public void onShutdownRequest() {
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
      // set progress to deliver to RM on next heartbeat
      /**
       * Date：2015/11/19
       * 原来：根据container的执行数量决定进度
       * 现在：AM作为long-running的service存在，所以一旦运行起来，进度就是100%
       */
      //float progress = (float) numCompletedContainers.get() / numTotalContainers;
      float progress = 1.0f;
      return progress;
    }

    // RM调用onError方法，说明出错，强制终止AM
    @Override
    public void onError(Throwable e) {
      done = true;
      amRMClient.stop();
    }
  }

  // 回调类，被NM调用来通知AM
  @VisibleForTesting
  static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

    private ConcurrentMap<ContainerId, Container> containers =
        new ConcurrentHashMap<ContainerId, Container>();
    private final ApplicationMaster applicationMaster;

    public NMCallbackHandler(ApplicationMaster applicationMaster) {
      this.applicationMaster = applicationMaster;
    }

    public void addContainer(ContainerId containerId, Container container) {
      containers.putIfAbsent(containerId, container);
    }

    // 当 NM 停止了一些 Containers 时，会调用改方法，把 Container 列表传给 AM
    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to stop Container " + containerId);
      }
      containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: id=" + containerId + ", status=" +
            containerStatus);
      }
      
      // ************************
      // to test what will output
      // ************************
      LOG.info("【Line 1038】Container Status: id=" + containerId + ", status=" +
              containerStatus);
    }

    // 当 NM 新启动了 Containers 时，会调用改方法，把 Container 列表传给AM
    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) { // <code>NMClientImpl<code>
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to start Container " + containerId);
      }
      
      /**
       * 一旦container启动，就立刻返回更改VM的状态
       * 因为如果等container执行完才更改状态的话，在启动VM时，container不会停止，VM状态也无法更改
       */
      Container container = containers.get(containerId);
      if (container != null) {
        applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
        Task task = null;
        try {
            task = pendingTaskQueue.take();
            List<String> sqlList = new LinkedList<String>();
            if (task.getTargetObjectType().equals("VirtualMachine")) {
                ContainerId containerID = vmToContainer.get(task.getTargetObjectId());
                // 如果执行完的container就是目前task对应的container，那么就更改vm的状态
                if (container.getId().equals(containerID)) {
                    String sql = null;
                    // 在deploy/start/stop时才会发生状态变化
                    if (task.getOperation().equals("deploy") || task.getOperation().equals("stop")) {
                        sql = "update virtual_machines set status = 'stopped' where id = " + task.getTargetObjectId();
                    }
                    else if (task.getOperation().equals("start")) {
                        sql = "update virtual_machines set status = 'running' where id = " + task.getTargetObjectId();
                    }
                    LOG.info("[Line 1031: ]update VirtualMachine's status: " + sql);
                    sqlList.add(sql);
                    // TODO 暂时没有考虑job执行失败的情况
                    sql = "update jobs set status = 'finished' where id = " + task.getJobId();
                    sqlList.add(sql);
                    // 不能使用外边的con，原因在于con必须设置为static类型才可以，而这种做法会导致外边的con无法使用
                    ConnectDataBase conn = new ConnectDataBase();
                    conn.executeUpdates(sqlList);
                    conn = null;
                    //System.gc();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
      }
      ApplicationMaster.publishContainerStartEvent(
          applicationMaster.timelineClient, container,
          applicationMaster.domainId, applicationMaster.appSubmitterUgi);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start Container " + containerId);
      containers.remove(containerId);
      applicationMaster.numCompletedContainers.incrementAndGet();
      applicationMaster.numFailedContainers.incrementAndGet();
    }

    @Override
    public void onGetContainerStatusError(
        ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop Container " + containerId);
      containers.remove(containerId);
    }
  }

  /**
   * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
   * that will execute the shell command.
   */
  private class LaunchContainerRunnable implements Runnable {

    // Allocated container
    Container container;

    NMCallbackHandler containerListener;

    /**
     * @param lcontainer Allocated container
     * @param containerListener Callback handler of the container
     */
    public LaunchContainerRunnable(
        Container lcontainer, NMCallbackHandler containerListener) {
      this.container = lcontainer;
      this.containerListener = containerListener;
    }
    
    @Override
    /**
     * Connects to CM, sets up container launch context 
     * for shell command and eventually dispatches the container 
     * start request to the CM. 
     */
    /**
     * 线程的执行方法
     * 获取shell脚本的地址，然后实例化ContainerLaunchContext，然后提交给NM执行container！
     */ 
    public void run() {
      LOG.info("Setting up container launch container for containerid="
          + container.getId());

      // Set the local resources
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

      // The container for the eventual shell commands needs its own local
      // resources too.
      // In this scenario, if a shell script is specified, we need to have it
      // copied and made available to the container.
      if (!scriptPath.isEmpty()) {
        Path renamedScriptPath = null;
        if (Shell.WINDOWS) {
          renamedScriptPath = new Path(scriptPath + ".bat");
        } else {
          renamedScriptPath = new Path(scriptPath + ".sh");
        }

        try {
          // rename the script file based on the underlying OS syntax.
          renameScriptFile(renamedScriptPath);
        } catch (Exception e) {
          LOG.error(
              "Not able to add suffix (.bat/.sh) to the shell script filename",
              e);
          // We know we cannot continue launching the container
          // so we should release it.
          numCompletedContainers.incrementAndGet();
          numFailedContainers.incrementAndGet();
          return;
        }

        URL yarnUrl = null;
        try {
          yarnUrl = ConverterUtils.getYarnUrlFromURI(
            new URI(renamedScriptPath.toString()));
        } catch (URISyntaxException e) {
          LOG.error("Error when trying to use shell script path specified"
              + " in env, path=" + renamedScriptPath, e);
          // A failure scenario on bad input such as invalid shell script path
          // We know we cannot continue launching the container
          // so we should release it.
          // TODO
          numCompletedContainers.incrementAndGet();
          numFailedContainers.incrementAndGet();
          return;
        }
        LocalResource shellRsrc = LocalResource.newInstance(yarnUrl,
          LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
          shellScriptPathLen, shellScriptPathTimestamp);
        localResources.put(Shell.WINDOWS ? ExecBatScripStringtPath :
            ExecShellStringPath, shellRsrc);
        shellCommand = Shell.WINDOWS ? windows_command : linux_bash_command;
      }

      // Set the necessary command to execute on the allocated container
      Vector<CharSequence> vargs = new Vector<CharSequence>(5);

      // Set executable command
      vargs.add(shellCommand);
      // Set shell script path
      if (!scriptPath.isEmpty()) {
        vargs.add(Shell.WINDOWS ? ExecBatScripStringtPath
            : ExecShellStringPath);
      }

      // Set args for the shell command if any
      vargs.add(shellArgs);
      // Add log redirect params
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

      // Get final commmand
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }

      List<String> commands = new ArrayList<String>();
      commands.add(command.toString());
      
      // test
      LOG.info("The final loacalResources : " + localResources);
      LOG.info("The final command : " + commands);
      LOG.info("The final shellEnv : " + shellEnv);

      /**
       *  至此，得到了最终要在container中执行的命令！
       *  例子：
       *  localResources:{ExecScript.sh=resource 
       *  					{ scheme: "hdfs" 
       *  						host: "test25" 
       *  						port: 9000 
       *  						file: "/user/hadoop/ivic/application_1431999596479_0024/ExecScript.sh" 
       *  					} 
       *  					size: 57 
       *  					timestamp: 1433075314903 
       *  					type: FILE 
       *  					visibility: APPLICATION
       *  				}
       *  commands: bash ExecScript.sh  1><LOG_DIR>/stdout 2><LOG_DIR>/stderr
       *  shellEnv: {}
       *  在NM接收到后，应该还要进行解析！！
       */
      
      // Set up ContainerLaunchContext, setting local resource, environment,
      // command and token for constructor.

      // Note for tokens: Set up tokens for the container too. Today, for normal
      // shell commands, the container in distribute-shell doesn't need any
      // tokens. We are populating them mainly for NodeManagers to be able to
      // download anyfiles in the distributed file-system. The tokens are
      // otherwise also useful in cases, for e.g., when one is running a
      // "hadoop dfs" command inside the ivic.
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
        localResources, shellEnv, commands, null, allTokens.duplicate(), null);
      // 把要执行的container保存在hashmap中
      containerListener.addContainer(container.getId(), container);
      
      // AM与NM通过NMClientAsync通信
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }

  private void renameScriptFile(final Path renamedScriptPath)
      throws IOException, InterruptedException {
    appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws IOException {
        FileSystem fs = renamedScriptPath.getFileSystem(conf);
        fs.rename(new Path(scriptPath), renamedScriptPath);
        return null;
      }
    });
    LOG.info("User " + appSubmitterUgi.getUserName()
        + " added suffix(.sh/.bat) to script file as " + renamedScriptPath);
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   * @return the setup ResourceRequest to be sent to RM
   */
  private ContainerRequest setupContainerAskForRM(int containerMemory, int containerVirtualCores) {
    // setup requirements for hosts
    // using * as any host will do for the ivic app
    // set the priority for the request
    // TODO - what is the range for priority? how to decide?
    Priority pri = Priority.newInstance(requestPriority);

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Resource.newInstance(containerMemory,
      containerVirtualCores);

    // 参数列表:Resource capability, String[] nodes, String[] racks, Priority priority
    ContainerRequest request = new ContainerRequest(capability, null, null,
        pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  private boolean fileExist(String filePath) {
    return new File(filePath).exists();
  }

  private String readContent(String filePath) throws IOException {
    DataInputStream ds = null;
    try {
      ds = new DataInputStream(new FileInputStream(filePath));
      return ds.readUTF();
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(ds);
    }
  }
  
  // 发布container启动的事件
  private static void publishContainerStartEvent(
      final TimelineClient timelineClient, Container container, String domainId,
      UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getId().toString());
    entity.setEntityType(DSEntity.DS_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter("user", ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(DSEvent.DS_CONTAINER_START.toString());
    event.addEventInfo("Node", container.getNodeId().toString());
    event.addEventInfo("Resources", container.getResource().toString());
    entity.addEvent(event);

    try {
      ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
        @Override
        public TimelinePutResponse run() throws Exception {
          return timelineClient.putEntities(entity);
        }
      });
    } catch (Exception e) {
      LOG.error("Container start event could not be published for "
          + container.getId().toString(),
          e instanceof UndeclaredThrowableException ? e.getCause() : e);
    }
  }

  // 发布container终止的事件
  private static void publishContainerEndEvent(
      final TimelineClient timelineClient, ContainerStatus container,
      String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getContainerId().toString());
    entity.setEntityType(DSEntity.DS_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter("user", ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(DSEvent.DS_CONTAINER_END.toString());
    event.addEventInfo("State", container.getState().name());
    event.addEventInfo("Exit Status", container.getExitStatus());
    entity.addEvent(event);

    try {
      ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
        @Override
        public TimelinePutResponse run() throws Exception {
          return timelineClient.putEntities(entity);
        }
      });
    } catch (Exception e) {
      LOG.error("Container end event could not be published for "
          + container.getContainerId().toString(),
          e instanceof UndeclaredThrowableException ? e.getCause() : e);
    }
  }

  private static void publishApplicationAttemptEvent(
      final TimelineClient timelineClient, String appAttemptId,
      DSEvent appEvent, String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(appAttemptId);
    entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter("user", ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setEventType(appEvent.toString());
    event.setTimestamp(System.currentTimeMillis());
    entity.addEvent(event);

    try {
      ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
        @Override
        public TimelinePutResponse run() throws Exception {
          return timelineClient.putEntities(entity);
        }
      });
    } catch (Exception e) {
      LOG.error("App Attempt "
          + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for "
          + appAttemptId.toString(),
          e instanceof UndeclaredThrowableException ? e.getCause() : e);
    }
  }
  
  //生产者线程，轮询数据库，读取pendingJob
  class LoopJobReader implements Runnable {
      ResultSet rs = null;
      String sql;
      public void run() {
          LOG.info("开始执行生产者进程！");
          while (true) {
              if (con == null) {
                  con = new ConnectDataBase();
              }
              // step1. schedule the pending jobs
              sql = "select * from jobs where status = 'pending' and user_id = " + userID;
              //LOG.info("读取pendingJob： " + sql);
              rs = con.executeQuery(sql);
              try {
                  while (rs.next()) {
                      LOG.info("开始解析Job！");
                      //jobId = rs.getString(0);
                      // TODO 将job分解为多个task,并将task放入队列;构建一个task队列和一个failed task的hashmap，相同的failed的job id的task至保存一个即可
                      TaskGenerator(con, rs);

                      // TODO 存在通过改ResultSet来更改数据库记录的方法
                      // 如果有将task保存下来的功能则存在中间状态转换，但是如果将job解析完成后直接请求container执行咋办？   
                      //sql = "update jobs set status = 'scheduling' where id = " + jobId;
                      //con.executeUpdate(sql);
                      //LOG.info("update jobs status success!");
                  }
              }
              catch (SQLException e) {
                  // TODO 原vshecd中将job.status=error
                  // LOG.info("update jobs status failed!");
                  e.printStackTrace();
              }
             
              // TODO 这一步在task状态执行完后，由执行结果修改job和vm的状态
              // step2.update status of finished jobs
             
              // TODO 这一步和多线程有关，现在只实现单线程
              // step3. handle the pending tasks
             
              // step4. sleep for 1 sec
              // sleep(1);
              try {
                  Thread.currentThread();
                  Thread.sleep(1000);
              } catch (InterruptedException e) {
                  // TODO Auto-generated catch block
                  e.printStackTrace();
              }
          }
      }
  }
  
  /**
   * @param pending状态的job集合
   *   => Vcluster: portal将一个vcluster当成是多个vm的job，循环发送job，还是按多个vm的形式，因为对每个vm，后台都需要vm_setting
   * options: 只在 undeployVM 时用来区分是否删除vdisk
   * @throws SQLException 
   * @throws NumberFormatException 
   */
  private void TaskGenerator(ConnectDataBase con, ResultSet rs) throws NumberFormatException, SQLException {
      if (con == null) {
          con = new ConnectDataBase();
      }
	  LOG.info("[Function TaskGenerator] get a new job to handler.");
	  String jobId = rs.getString("id");
	  String jobObject = rs.getString("target_object_type");
	  String jobObjectId = rs.getString("target_object_id");
	  String jobOperation = rs.getString("operation");
	  String jobOption = rs.getString("options");
	  
	  //containerMemory = 10;
      //containerVirtualCores = 1;
	  
	  // VM has operations: deploy, start, stop, undeploy
	  if (jobObject.equals("VirtualMachine")) {
	      LOG.info("*****genVirtaulMachineTask: " + con + "," + jobId + "," + jobOperation + "," + jobObjectId + "," + jobOption);
	      genVirtaulMachineTask(con, jobId, jobOperation, jobObjectId, jobOption, null);
	  }
	  else if (jobObject.equals("Vdisk")) {
          
      }
	  else {// TODO - 增加其他类型task的分解过程
		  LOG.error("Target object type is wrong!");
	  }
  }
  
  // 主要是生成task，保存在数据库和queue中，具体处理等到最后再进行。
  private void genVirtaulMachineTask(ConnectDataBase con, String jobId, String jobOperation, String jobObjectId, String jobOption, String dependingTaskId) throws SQLException {
      if (con == null) {
          con = new ConnectDataBase();
      }
      String vmSetting = null;
      String uuid = null;
      String sql = null;
      String shellArgs = null;
      Task task = new Task();
      // 保存参数，和原数据库保持一致，都是一个hash表
      //HashMap<String, String> map = new HashMap<String, String>();
      //map.put("uuid", uuid);

      // 默认值
      int containerMemory = 10;
      int containerVirtualCores = 1;
      
      /**
       *  deploy和edit时，只是把资源信息写入xml中，并没有真正使用；资源使用发生在虚拟机运行时(start)，所以资源量重新赋值
       *  所以此处有两种选择，有待考证：
       *  第一种：原来的思路，deploy和edit时，只需要一个很小的container能够执行命令就行，反正最终container执行完以后都要被回收；只有在start时才分配完整的资源
       *  第二种：现在的思路：deploy/edit/start时，都按照全部的资源量申请，原因在于，防止后边启动VM后资源不够！但是在deploy/edit后，资源还是会全部被回收；
       *  对于startVM，启动后container一直运行，只有在VM停止后，container才停止【这也是把AM设计为long-time running的原因！（原来的一个AM往往对应一个job，等）】
       */
      sql = "select id, uuid, hostname, vnc_password, mem_total, vcpu, vm_temp_id, disk_dev_type from virtual_machines where id = " + jobObjectId;
      LOG.info("******" + sql);
      ResultSet rSet = con.executeQuery(sql);
      while (rSet.next()) {
          uuid = rSet.getString("uuid");
          if (jobOperation.equals("deploy") || jobOperation.equals("start") || jobOperation.equals("edit")) {
              vmSetting = generateVmSetting(con, rSet);
              // 内存单位为KB，需转换为MB
              String vmem = rSet.getString("mem_total");
              String vcpu = rSet.getString("vcpu");
              LOG.info("*********** vMem: " + vmem);
              LOG.info("*********** vCPU: " + vcpu);
              containerMemory = Integer.parseInt(vmem) / 1024;
              containerVirtualCores = Integer.parseInt(vcpu);
              if (jobOperation.equals("start")) {
                  // 将执行的方法名、uuid和vm_setting作为执行python函数的参数
                  // 加转义双重的转义字符是因为，最终拼成的命令为：
                  // `exec /bin/bash -c "bash ExecScript.sh deplyVM "uuid-test1" "<vNode>...</vNode>" 1>/xxx/logs/userlogs/application_xxx/container_xxx/stdout 2>/xxx/stderr "'
                  shellArgs = vmMethodMapping.get(jobOperation) + " \\\"" + uuid + "\\\"";
              }
              else {
                  shellArgs = vmMethodMapping.get(jobOperation) + " \\\"" + uuid + "\\\" \\\"" + vmSetting + "\\\"";
              }
              LOG.info("############ shellArgs: " + shellArgs);
          }
          else if (jobOperation.equals("stop")) { // 如果options为destroy，则shutdown即强制关闭VM;若为NULL，则是正常关闭
              shellArgs = vmMethodMapping.get(jobOperation) + " " + uuid + " " + jobOption;
              LOG.info("############ shellArgs: " + shellArgs);
              //map.put("stopMode", jobOption);
          }
          else if (jobOperation.equals("undeploy")) { 
              task.setSoapMethod("undeployVM");
              // 数据库中的option字段为“{“delete_vdisk => yes/no”}”,是一个hash表
              if (jobOption.split("=>")[1].equals("yes")) {
                  shellArgs = vmMethodMapping.get(jobOperation) + " " + uuid + " yes";
                  //map.put("delete", "yes");
              }
              else {
                  shellArgs = vmMethodMapping.get(jobOperation) + " " + uuid;
              }
              LOG.info("############ shellArgs: " + shellArgs);
          }
      }
      
      // 生成task对象，保存在队列和数据库中
      task.setUuid(UUID.randomUUID().toString());
      task.setJobId(jobId);
      //task.setDependingTaskId(dependingTaskId);
      task.setTargetObjectId(jobObjectId);
      task.setTargetObjectType("VirtualMachine");
      task.setOperation(jobOperation);
      task.setMemory(containerMemory);
      task.setVcpu(containerVirtualCores);
      //task.setSoapMethod(vmMethodMapping.get(jobOperation));
      //task.setSoapArgs(map.toString());
      task.setShellArgs(shellArgs);
      //task.setStatus(dependingTaskId == null ? "pending" : "waiting");
      //task.setStatus("pending"); // TODO 似乎没啥用。。
      //task.setTitle(task.getOperation() + " vmi " + task.getTargetObjectId());
      SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");     
      String createdTime = sDateFormat.format(new java.util.Date());
      task.setCreatedTime(createdTime);
      // 将task保存在队列中
      try {
          taskQueue.put(task); // 队列满，线程阻塞，直到queue有空间再继续
      } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      //taskQueue.put(task);
      // job.status: pending => scheduling
      sql = "update jobs set status = 'scheduling' where id = " + jobId;
      con.executeUpdate(sql);
      LOG.info("*update job's status from pending to scheduling: " + sql);
  }
  
  // ****************************************************
  // 根据vm的查询记录生成vm_setting字段
  // 1.先生成一个xml文档
  // 2.将xml转换为字符串
  // ****************************************************  
  private static String generateVmSetting(ConnectDataBase con, ResultSet rs) {
      if (con == null) {
          con = new ConnectDataBase();
      }
      String xmlStr = null;
      String sql;
      ResultSet rSet;
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = null;
      try {
          builder = dbf.newDocumentBuilder();
      } catch (Exception e) {
          e.printStackTrace();
      }
      Document doc = builder.newDocument();

      Element root = doc.createElement("vNode");
      doc.appendChild(root); // 将根元素添加到文档上

      try {	      
          Element hostName = doc.createElement("Hostname");
          hostName.setTextContent(rs.getString("hostname"));
          root.appendChild(hostName);

          Element password = doc.createElement("Password");
          password.setTextContent(rs.getString("vnc_password"));
          root.appendChild(password);

          Element mem = doc.createElement("Mem");
          mem.setTextContent(rs.getString("mem_total"));
          root.appendChild(mem);

          Element vcpu = doc.createElement("vCPU");
          vcpu.setTextContent(rs.getString("vcpu"));
          root.appendChild(vcpu);
          // ******************************************
          // 以下都是vm_temps表中的信息
          // ******************************************
          if (rs.getString("vm_temp_id") != null) {
              sql = "select url, os_type, os_distribution, os_release, os_kernel, os_packages from vm_temps where id = " + rs.getString("vm_temp_id");
              ResultSet set = con.executeQuery(sql);
              while (set.next()) {
                  Element vTemplateRef = doc.createElement("vTemplateRef");
                  vTemplateRef.setTextContent(set.getString("url"));
                  root.appendChild(vTemplateRef);
			    	  
                  Element os = doc.createElement("OS");
                  root.appendChild(os);
			    	  
                  Element type = doc.createElement("Type");
                  type.setTextContent(set.getString("os_type"));
                  os.appendChild(type);
			    	  
                  Element distribution = doc.createElement("Distribution");
                  distribution.setTextContent(set.getString("os_distribution"));
                  os.appendChild(distribution);
			    	  
                  Element release = doc.createElement("Release");
                  release.setTextContent(set.getString("os_release"));
                  os.appendChild(release);
			    	  
                  Element kernel = doc.createElement("Kernel");
                  kernel.setTextContent(set.getString("os_kernel"));
                  os.appendChild(kernel);
			    	  
                  Element packages = doc.createElement("Packages");
                  packages.setTextContent(set.getString("os_packages"));
                  os.appendChild(packages);
              }
          }
          else {
              Element devType = doc.createElement("DevType");
              devType.setTextContent(rs.getString("disk_dev_type"));
              root.appendChild(devType);
          }
		      
          // **************************************************
          // 根据vm_id读取vdisks表的信息
          // TODO 原来需要知道把虚拟机创建的地点，现在应该不需要
          // 这里将volume和cdrom类型的vdisk同时取出，并按type和position排序，
          // **************************************************
          int index = 0;
          sql = "select uuid, vdisk_type, img_type, base_id, size from vdisks where virtual_machine_id = " + rs.getString("id") + " order by img_type, position";
          rSet = con.executeQuery(sql);
          while (rSet.next()) {
              Element vdisk = doc.createElement("vDisk");
              vdisk.setAttribute("id", "\\\'" + index + "\\\'");
              root.appendChild(vdisk);
              
              Element uuid = doc.createElement("UUID");
              uuid.setTextContent(rSet.getString("uuid"));
              vdisk.appendChild(uuid);
				  
              Element type = doc.createElement("Type");
              if (rSet.getString("vdisk_type").equals("volumn"))
                  type.setTextContent(rSet.getString("img_type"));
              else
                  type.setTextContent("cdrom");
              vdisk.appendChild(type);
				  
              // TODO 路径这里先写死！
              Element path = doc.createElement("Path");
              if (rSet.getString("vdisk_type").equals("volumn"))
                  path.setTextContent("/var/lib/ivic/vstore/" + rSet.getString("uuid") + ".img");
              else
                  path.setTextContent("/var/lib/ivic/vstore/" + rSet.getString("uuid") + ".iso");
              vdisk.appendChild(path);
				  
              if (rs.getString("vm_temp_id") != null && rSet.getString("base_id") != null) {
                  Element basePath = doc.createElement("BasePath");
                  // TODO 在vdisk表中存在base_id，但是需要知道base的uuid，所以，要么重新查询数据库，要么在vdisk表中增加一个字段；先按前者查询
                  sql = "select * from vdisks where id = " + rSet.getString("base_id");
                  ResultSet set = con.executeQuery(sql);
                  while (set.next()) {
                      if (rSet.getString("vdisk_type").equals("volumn"))
                          basePath.setTextContent("/var/lib/ivic/vstore/" + set.getString("uuid") + ".img");
                      else
                          basePath.setTextContent("/var/lib/ivic/vstore/" + set.getString("uuid") + ".iso");
                  }
                  vdisk.appendChild(basePath);
              }
				  
              if (rSet.getString("img_type").equals("raw") || rSet.getString("img_type").equals("rootfs")) {
                  Element size = doc.createElement("Size");
                  size.setTextContent(rSet.getString("size"));
                  vdisk.appendChild(size);
              }
              index++;
          }

          sql = "select vnics.*, vnets.vswitch_id from vnics, vnets where vnics.virtual_machine_id = " + rs.getString("id") + " and vnics.vnet_id = vnets.id";
          rSet = con.executeQuery(sql);
          while (rSet.next()) {
              Element nic = doc.createElement("NIC");
              nic.setAttribute("id", "\\\'" + (rSet.getRow() - 1) + "\\\'");
              root.appendChild(nic);

              Element mac = doc.createElement("MAC");
              mac.setTextContent(rSet.getString("mac_address"));
              nic.appendChild(mac);

              Element addr = doc.createElement("Address");
              addr.setTextContent(rSet.getString("ip_address"));
              nic.appendChild(addr);

              Element netmask = doc.createElement("Netmask");
              netmask.setTextContent(rSet.getString("netmask"));
              nic.appendChild(netmask);
              
              Element gateway = doc.createElement("GateWay");
              gateway.setTextContent(rSet.getString("gateway"));
              nic.appendChild(gateway);
              
              Element dns = doc.createElement("DNS");
              dns.setTextContent("8.8.8.8");
              nic.appendChild(dns);
              
              Element vswitch = doc.createElement("vSwitchRef");
              if (rSet.getString("vswitch_id") != null) {
                  sql = "select uuid from vswitches where id = " + rSet.getString("vswitch_id");
                  ResultSet set = con.executeQuery(sql);
                  while (set.next()) {
                      vswitch.setTextContent(set.getString(1));
                  }
              }
              nic.appendChild(vswitch);
              
              Element type = doc.createElement("vnetType");
              type.setTextContent(rSet.getString("gateway"));
              nic.appendChild(type);
          }
      } catch (DOMException e) {
          e.printStackTrace();
      } catch (SQLException e) {
          e.printStackTrace();
      }
      
      TransformerFactory tf = TransformerFactory.newInstance();      
      Transformer t;
      try {
          t = tf.newTransformer();
          t.setOutputProperty("encoding", "UTF-8");//解决中文问题，试过用GBK不行   
          t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
          ByteArrayOutputStream  bos  =  new  ByteArrayOutputStream();     
          t.transform(new DOMSource(doc), new StreamResult(bos));
          xmlStr = bos.toString();
          return xmlStr;
      } catch (Exception e) {
          e.printStackTrace();
      }
	    
      return xmlStr;
  }
  
  // TODO 这里先用单线程实现，后期可以改为多线程
  class TaskRunner implements Runnable {
      // private BlockingQueue<Task> queue;
      //private ConcurrentLinkedQueue<Task> queue1;
      
      //public TaskRunner(BlockingQueue<Task> queue) {
         // LOG.info("********create TaskRunner!");
         // this.queue = queue;
      //}
      
      //ConnectDataBase con = new ConnectDataBase();
      String sql;
      public void run() {
          while(true) {
              Task task = null;
              try {
                  LOG.info("get a task from taskQueue.");
                  // 队列为空，线程阻塞进入等待状态，直到有新对象加入为止
                  task = taskQueue.take();
                  // 取出要处理的task，然后申请container并将task保存在中间队列pendingTaskQueue中
                  pendingTaskQueue.put(task);
                  LOG.info("Begin to execute task.");
                  doTask(task);
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
          }
      }
      
      private void doTask(Task task) {
          if (con == null) {
              con = new ConnectDataBase();
          }
          // 更新vm/vdisk的状态，变为执行中的状态
          if (task.getTargetObjectId() != null) {
              sql = "update " + tables.get(task.getTargetObjectType()) + " set status = '" + operationToState.get(task.getOperation()) + "' where id = " + task.getTargetObjectId();
              LOG.info("update VM's status: " + sql);
              con.executeUpdate(sql);
          }
          int containerMemory = task.getMemory();
          int containerVirtualCores = task.getVcpu();
          
          // TODO 这里对于创建的VM不适用，公有云环境下，尽量保证资源可用，以及所提供最大资源足够大
          // A resource ask cannot exceed the max.
          if (containerMemory > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                + " Using max value." + ", specified=" + containerMemory + ", max="
                + maxMem);
            containerMemory = maxMem;
          }

          if (containerVirtualCores > maxVCores) {
            LOG.info("Container virtual cores specified above max threshold of cluster."
                + " Using max value." + ", specified=" + containerVirtualCores + ", max="
                + maxVCores);
            containerVirtualCores = maxVCores;
          }

          shellArgs = task.getShellArgs();
          
          // Setup ask for containers from RM
          // Send request for containers to RM
          // Until we get our fully allocated quota, we keep on polling RM for
          // containers
          // Keep looping until all the containers are launched and shell script
          // executed on them ( regardless of success/failure).
          // 申请资源的时候只和mem/vcpu有关，shell命令的其他内容等得到container后才能用到
          ContainerRequest containerAsk = setupContainerAskForRM(containerMemory, containerVirtualCores);
          extracted(containerAsk);
          
          publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                  DSEvent.DS_APP_ATTEMPT_END, domainId, appSubmitterUgi);
      }

    @SuppressWarnings("unchecked")
    private void extracted(ContainerRequest containerAsk) {
        amRMClient.addContainerRequest(containerAsk);
    }
  }
  
}