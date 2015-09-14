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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.activiti.engine.ActivitiException;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.impl.SchemaOperationsProcessEngineBuild;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.db.DbSqlSessionFactory;
import org.activiti.engine.impl.interceptor.CommandInterceptor;
import org.activiti.engine.impl.persistence.StrongUuidGenerator;
import org.activiti.engine.impl.util.IoUtil;
import org.activiti.impl.db.ExecuteSchemaOperationCommand;
import org.activiti.tenant.IdentityManagementService;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Joram Barrez
 */
public class MultiTenantProcessEngineConfiguration extends ProcessEngineConfigurationImpl {
  
  private static final Logger logger = LoggerFactory.getLogger(MultiTenantProcessEngineConfiguration.class);
  
  protected IdentityManagementService identityManagementService;
  
  protected String multiTenantDatabaseSchemaUpdate;
  
  protected List<MultiTenantDataSourceConfiguration> datasourceConfiguration;
  protected Map<String, MultiTenantDataSourceConfiguration> tenantToDataSourceConfigurationMap = new HashMap<String, MultiTenantDataSourceConfiguration>();
  
  protected MultiTenantDbSqlSessionFactory multiTenantDbSqlSessionFactory;

  protected Map<String, DataSource> datasources = new HashMap<String, DataSource>();

  public MultiTenantProcessEngineConfiguration() {
    // Using the UUID generator, as otherwise the ids are pulled from a global pool of ids, backed by
    // a database table. Which is impossible with a mult-database-schema setup.
    this.idGenerator = new StrongUuidGenerator();
    
    // TODO: what about the job executor?
    this.setAsyncExecutorActivate(false);
    this.setAsyncExecutorEnabled(false);
    this.setJobExecutorActivate(false);
    
  }
  
  @Override
  protected CommandInterceptor createTransactionInterceptor() {
    return null;
  }

  @Override
  protected void initDataSource() {
    
    // Mapping the list to an easier to use map
    for (MultiTenantDataSourceConfiguration datasourceConfigurations : datasourceConfiguration) {
      tenantToDataSourceConfigurationMap.put(datasourceConfigurations.getTenantId(), datasourceConfigurations);
    }
    datasourceConfiguration = null; // Not used anymore later on, so safe to set it to null
    
    // Getting the datasources from the config
    for (String tenantId : tenantToDataSourceConfigurationMap.keySet()) {
      DataSource dataSource = tenantToDataSourceConfigurationMap.get(tenantId).getDataSource();
      datasources.put(tenantId, dataSource);
    }
  }
  
  @Override
  protected DbSqlSessionFactory createDbSqlSessionFactory() {
    multiTenantDbSqlSessionFactory = new MultiTenantDbSqlSessionFactory(identityManagementService);
    for (String tenantId : tenantToDataSourceConfigurationMap.keySet()) {
      multiTenantDbSqlSessionFactory.addDatabaseType(tenantId, tenantToDataSourceConfigurationMap.get(tenantId).getDatabaseType());
    }
    return multiTenantDbSqlSessionFactory;
  }
  
  @Override
  protected void initSqlSessionFactory() {
    
    MultiTenantSqlSessionFactory multiTenantSqlSessionFactory = new MultiTenantSqlSessionFactory(identityManagementService);
    this.sqlSessionFactory = multiTenantSqlSessionFactory;
    
    for (String tenantId : identityManagementService.getAllTenants()) {
      
      logger.info("Initializing sql session factory for tenant " + tenantId);
       
      InputStream inputStream = null;
      try {
        inputStream = getMyBatisXmlConfigurationStream();

        DataSource dataSource = datasources.get(tenantId);
        String databaseType = tenantToDataSourceConfigurationMap.get(tenantId).getDatabaseType();
        
        Environment environment = new Environment("default", transactionFactory, dataSource);
        Reader reader = new InputStreamReader(inputStream);
        Properties properties = new Properties();
        properties.put("prefix", databaseTablePrefix);
        if (tenantToDataSourceConfigurationMap.get(tenantId).getDatabaseType() != null) {
          properties.put("limitBefore", DbSqlSessionFactory.databaseSpecificLimitBeforeStatements.get(databaseType));
          properties.put("limitAfter", DbSqlSessionFactory.databaseSpecificLimitAfterStatements.get(databaseType));
          properties.put("limitBetween", DbSqlSessionFactory.databaseSpecificLimitBetweenStatements.get(databaseType));
          properties.put("limitOuterJoinBetween", DbSqlSessionFactory.databaseOuterJoinLimitBetweenStatements.get(databaseType));
          properties.put("orderBy", DbSqlSessionFactory.databaseSpecificOrderByStatements.get(databaseType));
          properties.put("limitBeforeNativeQuery", ObjectUtils.toString(DbSqlSessionFactory.databaseSpecificLimitBeforeNativeQueryStatements.get(databaseType)));
        }

        Configuration configuration = initMybatisConfiguration(environment, reader, properties);
        multiTenantSqlSessionFactory.addConfig(tenantId, configuration);
        
      } catch (Exception e) {
        throw new ActivitiException("Error while building ibatis SqlSessionFactory: " + e.getMessage(), e);
      } finally {
        IoUtil.closeSilently(inputStream);
      }
    }
  }
  
  @Override
  public ProcessEngine buildProcessEngine() {
    
    // Disable schema creation/validation by setting it to null.
    // We'll do it manually, see buildProcessEngine() method (hence why it's copied first)
    this.multiTenantDatabaseSchemaUpdate = this.databaseSchemaUpdate;
    this.databaseSchemaUpdate = null; 
    
    ProcessEngine processEngine = super.buildProcessEngine();
    
    for (String tenantId : identityManagementService.getAllTenants()) {
      logger.info("creating/validating database schema for tenant " + tenantId);
      
      identityManagementService.setCurrentTenantId(tenantId);
      getCommandExecutor().execute(getSchemaCommandConfig(), new ExecuteSchemaOperationCommand(multiTenantDatabaseSchemaUpdate));
    }
    
    identityManagementService.clearCurrentTenantId();
    
    return processEngine;
  }
  
  public IdentityManagementService getIdentityManagementService() {
    return identityManagementService;
  }

  public void setIdentityManagementService(IdentityManagementService identityManagementService) {
    this.identityManagementService = identityManagementService;
  }

  public List<MultiTenantDataSourceConfiguration> getDatasourceConfigurations() {
    return datasourceConfiguration;
  }

  public void setDatasourceConfigurations(List<MultiTenantDataSourceConfiguration> datasourceConfigurations) {
    this.datasourceConfiguration = datasourceConfigurations;
  }
  
}