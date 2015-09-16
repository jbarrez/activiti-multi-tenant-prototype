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
package org.activiti;

import javax.sql.DataSource;

import org.activiti.datasource.TenantAwareDataSource;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.interceptor.CommandInterceptor;
import org.activiti.engine.impl.persistence.StrongUuidGenerator;
import org.activiti.impl.db.ExecuteSchemaOperationCommand;
import org.activiti.tenant.TenantInfoHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Joram Barrez
 */
public class MultiTenantProcessEngineConfigurationV2 extends ProcessEngineConfigurationImpl {
  
  private static final Logger logger = LoggerFactory.getLogger(MultiTenantProcessEngineConfigurationV2.class);
  
  protected TenantInfoHolder tenantInfoHolder;
  protected TenantAwareDataSource tenantAwareDataSource;
  protected String multiTenantDatabaseSchemaUpdate;
  protected boolean booted;
  
  public MultiTenantProcessEngineConfigurationV2(TenantInfoHolder tenantInfoHolder) {
    
    this.tenantInfoHolder = tenantInfoHolder;
    
    // Using the UUID generator, as otherwise the ids are pulled from a global pool of ids, backed by
    // a database table. Which is impossible with a mult-database-schema setup.
    this.idGenerator = new StrongUuidGenerator();
    
    // TODO: what about the job executor?
    this.setAsyncExecutorActivate(false);
    this.setAsyncExecutorEnabled(false);
    this.setJobExecutorActivate(false);
    
    this.tenantAwareDataSource = new TenantAwareDataSource(tenantInfoHolder);
    this.dataSource = tenantAwareDataSource;
    
  }
  
  public void registerDataSource(String tenantId, DataSource dataSource) {
    tenantAwareDataSource.addDataSource(tenantId, dataSource);
    
    if (booted) {
      createTenantSchema(tenantId);
    }
  }
  
  @Override
  public ProcessEngine buildProcessEngine() {
    
    // Disable schema creation/validation by setting it to null.
    // We'll do it manually, see buildProcessEngine() method (hence why it's copied first)
    this.multiTenantDatabaseSchemaUpdate = this.databaseSchemaUpdate;
    this.databaseSchemaUpdate = null; 
    
    ProcessEngine processEngine = super.buildProcessEngine();
    
    for (String tenantId : tenantInfoHolder.getAllTenants()) {
      createTenantSchema(tenantId);
    }
    tenantInfoHolder.clearCurrentTenantId();
    
    booted = true;
    return processEngine;
  }

  protected void createTenantSchema(String tenantId) {
    logger.info("creating/validating database schema for tenant " + tenantId);
    tenantInfoHolder.setCurrentTenantId(tenantId);
    getCommandExecutor().execute(getSchemaCommandConfig(), new ExecuteSchemaOperationCommand(multiTenantDatabaseSchemaUpdate));
  }
  
  @Override
  protected CommandInterceptor createTransactionInterceptor() {
    return null;
  }
  
}
