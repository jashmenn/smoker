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

package smoker;

import jline.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.hive.cli.*;

import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.shims.ShimLoader;

public class QueryRunner {

  private LogHelper console;
  private Configuration conf;
  private CliSessionState ss;

  public QueryRunner() {
    Log LOG = LogFactory.getLog("QueryRunner");
    console = new LogHelper(LOG);

    // NOTE: It is critical to do this here so that log4j is reinitialized before
    // any of the other core hive classes are loaded
    SessionState.initHiveLog4j();
 
    ss = new CliSessionState(new HiveConf(SessionState.class));

    boot();
  }


  public void boot() {
   
    // todo allow overriding these
    ss.in = System.in;
    try {
      ss.out = new PrintStream(System.out, true, "UTF-8");
      ss.err = new PrintStream(System.err, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      System.exit(3);
    }

    // set all properties specified via command line
    HiveConf conf = ss.getConf();
    for(Map.Entry<Object, Object> item: ss.cmdProperties.entrySet()) {
      conf.set((String) item.getKey(), (String) item.getValue());
    }
    
    if(!ShimLoader.getHadoopShims().usesJobShell()) {
      try {
        // hadoop-20 and above - we need to augment classpath using hiveconf components
        // see also: code in ExecDriver.java
        ClassLoader loader = conf.getClassLoader();
        String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
        if (StringUtils.isNotBlank(auxJars)) {
          loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","));
        }
        conf.setClassLoader(loader);
        Thread.currentThread().setContextClassLoader(loader);
      } catch (Exception e) {
        console.printError("Loading aux jars failed with exception " + e.getClass().getName() + ":" +   e.getMessage(),
                           "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }

    SessionState.start(ss);
  }
  
  public int runCmd(String cmd) {
    SessionState ss = SessionState.get();
    
    String cmd_trimmed = cmd.trim();
    String[] tokens = cmd_trimmed.split("\\s+");
    String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
    int ret = 0;

    CommandProcessor proc = CommandProcessorFactory.get(tokens[0]);
    if(proc != null) {
      if(proc instanceof Driver) {
        Driver qp = (Driver) proc;
        PrintStream out = ss.out;
        long start = System.currentTimeMillis();

        ret = qp.run(cmd);
        if (ret != 0) {
          qp.close();
          return ret;
        }
        
        Vector<String> res = new Vector<String>();
        try {
          while (qp.getResults(res)) {
            for (String r:res) {
              out.println(r);
            }
            res.clear();
            if (out.checkError()) {
              break;
            }
          }
        } catch (IOException e) {
          console.printError("Failed with exception " + e.getClass().getName() + ":" +   e.getMessage(),
                             "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
          ret = 1;
        }
            
        int cret = qp.close();
        if (ret == 0) {
          ret = cret;
        }

        long end = System.currentTimeMillis();
        if (end > start) {
          double timeTaken = (double)(end-start)/1000.0;
          console.printInfo("Time taken: " + timeTaken + " seconds", null);
        }

      } else {
        ret = proc.run(cmd_1);
      }
    }

    return ret;
  }

  public static void main(String[] args) throws Exception {
    QueryRunner cli = new QueryRunner();
    cli.runCmd("describe web_data");
  }

}
