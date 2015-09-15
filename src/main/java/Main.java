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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.MultiTenantDataSourceConfiguration;
import org.activiti.MultiTenantProcessEngineConfiguration;
import org.activiti.MybatisMultiTenantDatasourceConfiguration;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.repository.Deployment;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.engine.task.Task;
import org.activiti.tenant.TenantInfoHolder;

/**
 * @author Joram Barrez
 */
public class Main {
  
  private static ProcessEngine processEngine;

  private static TenantInfoHolder identityManagementService;
  
  public static void main(String[] args) {
    
    DummyTenantInfoHolder dummyTenantInfoHolder = new DummyTenantInfoHolder();
    
    dummyTenantInfoHolder.addTenant("alfresco");
    dummyTenantInfoHolder.addUser("alfresco", "joram");
    dummyTenantInfoHolder.addUser("alfresco", "tijs");
    dummyTenantInfoHolder.addUser("alfresco", "paul");
    dummyTenantInfoHolder.addUser("alfresco", "yvo");
    
    dummyTenantInfoHolder.addTenant("acme");
    dummyTenantInfoHolder.addUser("acme", "raphael");
    dummyTenantInfoHolder.addUser("acme", "john");
    
    dummyTenantInfoHolder.addTenant("starkindustries");
    dummyTenantInfoHolder.addUser("starkindustries", "tony");
    
    
    // Booting up the Activiti Engine
    
    MultiTenantProcessEngineConfiguration config = new MultiTenantProcessEngineConfiguration();
    
    config.setDatabaseSchemaUpdate(MultiTenantProcessEngineConfiguration.DB_SCHEMA_UPDATE_DROP_CREATE);
    
    List<MultiTenantDataSourceConfiguration> datasourceConfigurations = new ArrayList<MultiTenantDataSourceConfiguration>();
    datasourceConfigurations.add(new MybatisMultiTenantDatasourceConfiguration("alfresco", 
        ProcessEngineConfigurationImpl.DATABASE_TYPE_H2, "jdbc:h2:mem:activiti-alfresco;DB_CLOSE_DELAY=1000", "sa", "", "org.h2.Driver"));
    datasourceConfigurations.add(new MybatisMultiTenantDatasourceConfiguration("acme", 
        ProcessEngineConfigurationImpl.DATABASE_TYPE_H2, "jdbc:h2:mem:activiti-acme;DB_CLOSE_DELAY=1000", "sa", "", "org.h2.Driver"));
    datasourceConfigurations.add(new MybatisMultiTenantDatasourceConfiguration("starkindustries", 
        ProcessEngineConfigurationImpl.DATABASE_TYPE_MYSQL, "jdbc:mysql://127.0.0.1:3306/starkindustries?characterEncoding=UTF-8", "alfresco", "alfresco", "com.mysql.jdbc.Driver"));
    config.setDataSourceConfigurations(datasourceConfigurations);
    
    config.setTenantInfoHolder(dummyTenantInfoHolder);
    identityManagementService = dummyTenantInfoHolder;
    
    processEngine = config.buildProcessEngine();
    
    // Starting process instances for a few tenants
    
    startProcessInstance("joram");
    startProcessInstance("joram");
    startProcessInstance("joram");
    startProcessInstance("raphael");
    startProcessInstance("raphael");
    startProcessInstance("tony");
    
    // Adding a new tenant
    dummyTenantInfoHolder.addTenant("dailyplanet");
    dummyTenantInfoHolder.addUser("dailyplanet", "louis");
    dummyTenantInfoHolder.addUser("dailyplanet", "clark");
    
    config.addMultiTenantDataSourceConfiguration(new MybatisMultiTenantDatasourceConfiguration("dailyplanet", 
        ProcessEngineConfigurationImpl.DATABASE_TYPE_MYSQL, "jdbc:mysql://127.0.0.1:3306/dailyplanet?characterEncoding=UTF-8", "alfresco", "alfresco", "com.mysql.jdbc.Driver"));
    
    // Start process instance with new tenant
    startProcessInstance("clark");
    startProcessInstance("clark");
    
    System.out.println();
    System.out.println("ALL DONE");
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
