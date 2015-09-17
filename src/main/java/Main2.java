/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.activiti.MultiTenantProcessEngineConfigurationV2;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.repository.Deployment;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.engine.task.Task;
import org.activiti.multitenant.job.ExecutorPerTenantAsyncExecutor;
import org.activiti.multitenant.job.SharedExecutorServiceAsyncExecutor;
import org.activiti.tenant.TenantInfoHolder;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Joram Barrez
 */
public class Main2 {
  
  private static ProcessEngine processEngine;

  private static TenantInfoHolder identityManagementService;
  
  public static void main(String[] args) throws Exception {
    
    org.h2.tools.Server.createWebServer("-web").start();
    
    DummyTenantInfoHolder tenantInfoHolder = new DummyTenantInfoHolder();
    
    tenantInfoHolder.addTenant("alfresco");
    tenantInfoHolder.addUser("alfresco", "joram");
    tenantInfoHolder.addUser("alfresco", "tijs");
    tenantInfoHolder.addUser("alfresco", "paul");
    tenantInfoHolder.addUser("alfresco", "yvo");
    
    tenantInfoHolder.addTenant("acme");
    tenantInfoHolder.addUser("acme", "raphael");
    tenantInfoHolder.addUser("acme", "john");
    
    tenantInfoHolder.addTenant("starkindustries");
    tenantInfoHolder.addUser("starkindustries", "tony");
    
    
    // Booting up the Activiti Engine
    
    MultiTenantProcessEngineConfigurationV2 config = new MultiTenantProcessEngineConfigurationV2(tenantInfoHolder);

    config.setDatabaseType(MultiTenantProcessEngineConfigurationV2.DATABASE_TYPE_H2);
    config.setDatabaseSchemaUpdate(MultiTenantProcessEngineConfigurationV2.DB_SCHEMA_UPDATE_DROP_CREATE);
    
    config.setAsyncExecutorEnabled(true);
    config.setAsyncExecutorActivate(true);
    
//    config.setAsyncExecutor(new ExecutorPerTenantAsyncExecutor(tenantInfoHolder));
    config.setAsyncExecutor(new SharedExecutorServiceAsyncExecutor(tenantInfoHolder));
    
    config.registerTenant("alfresco", createDataSource("jdbc:h2:mem:activiti-alfresco;DB_CLOSE_DELAY=1000", "sa", ""));
    config.registerTenant("acme", createDataSource("jdbc:h2:mem:activiti-acme;DB_CLOSE_DELAY=1000", "sa", ""));
    config.registerTenant("starkindustries", createDataSource("jdbc:h2:mem:activiti-stark;DB_CLOSE_DELAY=1000", "sa", ""));
    
    identityManagementService = tenantInfoHolder;
    
    processEngine = config.buildProcessEngine();
    
    // Starting process instances for a few tenants
    
    startProcessInstances("joram");
    startProcessInstances("joram");
    startProcessInstances("joram");
    startProcessInstances("raphael");
    startProcessInstances("raphael");
    startProcessInstances("tony");
    
    // Adding a new tenant
    tenantInfoHolder.addTenant("dailyplanet");
    tenantInfoHolder.addUser("dailyplanet", "louis");
    tenantInfoHolder.addUser("dailyplanet", "clark");
    
    config.registerTenant("dailyplanet", createDataSource("jdbc:h2:mem:activiti-daily;DB_CLOSE_DELAY=1000", "sa", ""));
    
    // Start process instance with new tenant
    startProcessInstances("clark");
    startProcessInstances("clark");
    
    // Move the clock 2 hours (jobs fire in one hour)
    config.getClock().setCurrentTime(new Date(config.getClock().getCurrentTime().getTime() + (2 * 60 * 60 * 1000)));
    
    processEngine.close();
    
    System.out.println();
    System.out.println("ALL DONE");
    System.exit(0); // Otherwise hikari won't go down. Needs a manual shutdown() or close() ...
  }
  
  private static DataSource createDataSource(String jdbcUrl, String jdbcUsername, String jdbcPassword) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    config.setUsername(jdbcUsername);
    config.setPassword(jdbcPassword);
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    return new HikariDataSource(config);
  }

  private static void startProcessInstances(String userId) {
    
    System.out.println();
    System.out.println("Starting process instance for user " + userId);
    
    identityManagementService.setCurrentUserId(userId);
    
    Deployment deployment = processEngine.getRepositoryService().createDeployment()
          .addClasspathResource("oneTaskProcess.bpmn20.xml")
          .addClasspathResource("jobTest.bpmn20.xml")
          .deploy();
    System.out.println("Process deployed! Deployment id is " + deployment.getId());
    
    Map<String, Object> vars = new HashMap<String, Object>();
    if (userId.equals("joram")) {
      vars.put("data", "Hello from Joram!");
    } else if (userId.equals("raphael")) {
      vars.put("data", "Hello from Raphael!");
    } else if (userId.equals("tony")){
      vars.put("data", "Hello from Iron man!");
    } else {
      vars.put("data", "Hello from Superman!");
    }
    
    ProcessInstance processInstance = processEngine.getRuntimeService().startProcessInstanceByKey("oneTaskProcess", vars);
    List<Task> tasks = processEngine.getTaskService().createTaskQuery().processInstanceId(processInstance.getId()).list();
    System.out.println("Got " + tasks.size() + " tasks");
    
    System.out.println("Got " + processEngine.getHistoryService().createHistoricProcessInstanceQuery().count() + " process instances in the system");
    
    // Start a Job
    processEngine.getRuntimeService().startProcessInstanceByKey("jobTest");
  }
  
}
