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
import org.activiti.tenant.IdentityManagementService;

/**
 * @author Joram Barrez
 */
public class Main {
  
  private static ProcessEngine processEngine;

  private static IdentityManagementService identityManagementService;
  
  public static void main(String[] args) {
    
    DummyIdentityManagementService dummyIdentityManagementService = new DummyIdentityManagementService();
    
    dummyIdentityManagementService.addTenant("alfresco");
    dummyIdentityManagementService.addUser("alfresco", "joram");
    dummyIdentityManagementService.addUser("alfresco", "tijs");
    dummyIdentityManagementService.addUser("alfresco", "paul");
    dummyIdentityManagementService.addUser("alfresco", "yvo");
    
    dummyIdentityManagementService.addTenant("acme");
    dummyIdentityManagementService.addUser("acme", "raphael");
    dummyIdentityManagementService.addUser("acme", "john");
    
    
    // Booting up the Activiti Engine
    
    MultiTenantProcessEngineConfiguration config = new MultiTenantProcessEngineConfiguration();
    
    config.setDatabaseSchemaUpdate(MultiTenantProcessEngineConfiguration.DB_SCHEMA_UPDATE_CREATE_DROP);
    
    List<MultiTenantDataSourceConfiguration> datasourceConfigurations = new ArrayList<MultiTenantDataSourceConfiguration>();
    datasourceConfigurations.add(new MybatisMultiTenantDatasourceConfiguration("alfresco", 
        ProcessEngineConfigurationImpl.DATABASE_TYPE_H2, "jdbc:h2:mem:activiti-Alfresco;DB_CLOSE_DELAY=1000", "sa", "", "org.h2.Driver"));
    datasourceConfigurations.add(new MybatisMultiTenantDatasourceConfiguration("acme", 
        ProcessEngineConfigurationImpl.DATABASE_TYPE_H2, "jdbc:h2:mem:activiti-Uni;DB_CLOSE_DELAY=1000", "sa", "", "org.h2.Driver"));
    config.setDatasourceConfigurations(datasourceConfigurations);
    
    config.setIdentityManagementService(dummyIdentityManagementService);
    identityManagementService = dummyIdentityManagementService;
    
    processEngine = config.buildProcessEngine();
    
    StartProcessInstance("joram");
    StartProcessInstance("raphael");
    
    System.out.println("TEST");
  }

  private static void StartProcessInstance(String userId) {
    
    System.out.println();
    System.out.println("Starting process instance for user " + userId);
    
    identityManagementService.setCurrentUserId(userId);
    
    Deployment deployment = processEngine.getRepositoryService().createDeployment().addClasspathResource("oneTaskProcess.bpmn20.xml").deploy();
    System.out.println("Process deployed! Deployment id is " + deployment.getId());
    
    Map<String, Object> vars = new HashMap<String, Object>();
    if (userId.equals("joram")) {
      vars.put("data", "Hello from Joram!");
    } else {
      vars.put("data", "Hello from Raphael!");
    }
    
    ProcessInstance processInstance = processEngine.getRuntimeService().startProcessInstanceByKey("oneTaskProcess", vars);
    List<Task> tasks = processEngine.getTaskService().createTaskQuery().processInstanceId(processInstance.getId()).list();
    System.out.println("Got " + tasks.size() + " tasks");
    
    System.out.println("Got " + processEngine.getHistoryService().createHistoricProcessInstanceQuery().count() + " process instances in the system");
  }

}
