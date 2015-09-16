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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.activiti.MultiTenantProcessEngineConfigurationV2;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.repository.Deployment;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.engine.task.Task;
import org.activiti.tenant.TenantInfoHolder;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Joram Barrez
 */
public class Main2 {
  
  private static ProcessEngine processEngine;

  private static TenantInfoHolder identityManagementService;
  
  public static void main(String[] args) {
    
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
    
    config.registerDataSource("alfresco", createDataSource("jdbc:h2:mem:activiti-alfresco;DB_CLOSE_DELAY=1000", "sa", ""));
    config.registerDataSource("acme", createDataSource("jdbc:h2:mem:activiti-acme;DB_CLOSE_DELAY=1000", "sa", ""));
    config.registerDataSource("starkindustries", createDataSource("jdbc:h2:mem:activiti-stark;DB_CLOSE_DELAY=1000", "sa", ""));
    
    identityManagementService = tenantInfoHolder;
    
    processEngine = config.buildProcessEngine();
    
    // Starting process instances for a few tenants
    
    startProcessInstance("joram");
    startProcessInstance("joram");
    startProcessInstance("joram");
    startProcessInstance("raphael");
    startProcessInstance("raphael");
    startProcessInstance("tony");
    
    // Adding a new tenant
    tenantInfoHolder.addTenant("dailyplanet");
    tenantInfoHolder.addUser("dailyplanet", "louis");
    tenantInfoHolder.addUser("dailyplanet", "clark");
    
    config.registerDataSource("dailyplanet", createDataSource("jdbc:h2:mem:activiti-daily;DB_CLOSE_DELAY=1000", "sa", ""));
    
    // Start process instance with new tenant
    startProcessInstance("clark");
    startProcessInstance("clark");
    
    System.out.println();
    System.out.println("ALL DONE");
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

  private static void startProcessInstance(String userId) {
    
    System.out.println();
    System.out.println("Starting process instance for user " + userId);
    
    identityManagementService.setCurrentUserId(userId);
    
    Deployment deployment = processEngine.getRepositoryService().createDeployment().addClasspathResource("oneTaskProcess.bpmn20.xml").deploy();
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
  }
  
}
