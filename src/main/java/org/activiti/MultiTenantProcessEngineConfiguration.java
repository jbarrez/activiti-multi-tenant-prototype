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
import org.activiti.multitenant.job.ExecutorPerTenantAsyncExecutor;
import org.activiti.multitenant.job.TenantAwareAsyncExecutor;
import org.activiti.multitenant.job.TenantAwareAsyncExecutorFactory;
import org.activiti.tenant.TenantInfoHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Joram Barrez
 */
public class MultiTenantProcessEngineConfiguration extends ProcessEngineConfigurationImpl {
  
  private static final Logger logger = LoggerFactory.getLogger(MultiTenantProcessEngineConfiguration.class);
  
  protected TenantInfoHolder tenantInfoHolder;
  protected TenantAwareAsyncExecutorFactory tenantAwareAyncExecutorFactory;
  protected boolean booted;
  
  public MultiTenantProcessEngineConfiguration(TenantInfoHolder tenantInfoHolder) {
    
    this.tenantInfoHolder = tenantInfoHolder;
    
    // Using the UUID generator, as otherwise the ids are pulled from a global pool of ids, backed by
    // a database table. Which is impossible with a mult-database-schema setup.
    
    // Also: it avoids the need for having a process definition cache for each tenant
    
    this.idGenerator = new StrongUuidGenerator();
    
    this.dataSource = new TenantAwareDataSource(tenantInfoHolder);
  }
  
  public void registerTenant(String tenantId, DataSource dataSource) {
    ((TenantAwareDataSource) super.getDataSource()).addDataSource(tenantId, dataSource);
    
    if (booted) {
      createTenantSchema(tenantId);
      
      if (isAsyncExecutorEnabled()) {
        createTenantAsyncJobExecutor(tenantId);
      }
    }
  }
  
  @Override
  protected void initAsyncExecutor() {
    
    if (asyncExecutor == null) {
      asyncExecutor = new ExecutorPerTenantAsyncExecutor(tenantInfoHolder);
    }
    
    super.initAsyncExecutor();
    
    if (asyncExecutor instanceof TenantAwareAsyncExecutor) {
      for (String tenantId : tenantInfoHolder.getAllTenants()) {
        ((TenantAwareAsyncExecutor) asyncExecutor).addTenantAsyncExecutor(tenantId, false); // false -> will be started later with all the other executors
      }
    }
  }
  
  @Override
  public ProcessEngine buildProcessEngine() {
    
    // Disable schema creation/validation by setting it to null.
    // We'll do it manually, see buildProcessEngine() method (hence why it's copied first)
    String originalDatabaseSchemaUpdate = this.databaseSchemaUpdate;
    this.databaseSchemaUpdate = null; 
    
    // Also, we shouldn't start the async executor until *after* the schema's have been created
    boolean originalIsAutoActivateAsyncExecutor = this.asyncExecutorActivate;
    this.asyncExecutorActivate = false;
    
    ProcessEngine processEngine = super.buildProcessEngine();
    
    // Reset to original values
    this.databaseSchemaUpdate = originalDatabaseSchemaUpdate;
    this.asyncExecutorActivate = originalIsAutoActivateAsyncExecutor;
    
    // Create tenant schema
    for (String tenantId : tenantInfoHolder.getAllTenants()) {
      createTenantSchema(tenantId);
    }
    
    // Start async executor
    if (asyncExecutor != null && originalIsAutoActivateAsyncExecutor) {
      asyncExecutor.start();
    }
    
    booted = true;
    return processEngine;
  }

  protected void createTenantSchema(String tenantId) {
    logger.info("creating/validating database schema for tenant " + tenantId);
    tenantInfoHolder.setCurrentTenantId(tenantId);
    getCommandExecutor().execute(getSchemaCommandConfig(), new ExecuteSchemaOperationCommand(databaseSchemaUpdate));
    tenantInfoHolder.clearCurrentTenantId();
  }
  
  protected void createTenantAsyncJobExecutor(String tenantId) {
    ((TenantAwareAsyncExecutor) asyncExecutor).addTenantAsyncExecutor(tenantId, isAsyncExecutorActivate() && booted);
  }
  
  @Override
  protected CommandInterceptor createTransactionInterceptor() {
    return null;
  }

  public TenantAwareAsyncExecutorFactory getTenantAwareAyncExecutorFactory() {
    return tenantAwareAyncExecutorFactory;
  }

  public void setTenantAwareAyncExecutorFactory(TenantAwareAsyncExecutorFactory tenantAwareAyncExecutorFactory) {
    this.tenantAwareAyncExecutorFactory = tenantAwareAyncExecutorFactory;
  }
  
}
