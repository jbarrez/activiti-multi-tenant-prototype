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
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.db.DbSqlSessionFactory;
import org.activiti.engine.impl.interceptor.CommandInterceptor;
import org.activiti.engine.impl.persistence.StrongUuidGenerator;
import org.activiti.engine.impl.util.IoUtil;
import org.activiti.impl.db.ExecuteSchemaOperationCommand;
import org.activiti.tenant.TenantInfoHolder;
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
  
  protected TenantInfoHolder tenantInfoHolder;
  
  protected String multiTenantDatabaseSchemaUpdate;
  
  protected List<MultiTenantDataSourceConfiguration> dataSourceConfigurations;
  
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
    // Getting the datasources from the config
    for(MultiTenantDataSourceConfiguration dataSourceConfig : dataSourceConfigurations) {
      datasources.put(dataSourceConfig.getTenantId(), dataSourceConfig.getDataSource());
    }
  }
  
  @Override
  protected DbSqlSessionFactory createDbSqlSessionFactory() {
    multiTenantDbSqlSessionFactory = new MultiTenantDbSqlSessionFactory(tenantInfoHolder);
    for(MultiTenantDataSourceConfiguration dataSourceConfig : dataSourceConfigurations) {
      multiTenantDbSqlSessionFactory.addDatabaseType(dataSourceConfig.getTenantId(), dataSourceConfig.getDatabaseType());
    }
    return multiTenantDbSqlSessionFactory;
  }
  
  @Override
  protected void initSqlSessionFactory() {
    
    MultiTenantSqlSessionFactory multiTenantSqlSessionFactory = new MultiTenantSqlSessionFactory(tenantInfoHolder);
    this.sqlSessionFactory = multiTenantSqlSessionFactory;
    
    for (String tenantId : tenantInfoHolder.getAllTenants()) {
      initSqlSessionFactoryForTenant(multiTenantSqlSessionFactory, tenantId);
    }
  }

  protected void initSqlSessionFactoryForTenant(MultiTenantSqlSessionFactory multiTenantSqlSessionFactory, String tenantId) {
    logger.info("Initializing sql session factory for tenant " + tenantId);
     
    // This could be optimized by a hack: http://www.jorambarrez.be/blog/2014/08/22/seriously-reducing-memory/
    // For each database type, the Mybatis configuration could be cached and reused.
    // But not applied it, as it's a quite dirty bit of hacking with relfection.
    
    InputStream inputStream = null;
    try {
      inputStream = getMyBatisXmlConfigurationStream();
      DataSource dataSource = datasources.get(tenantId);
      
      MultiTenantDataSourceConfiguration dataSourceConfiguration = null;
      for (MultiTenantDataSourceConfiguration d : dataSourceConfigurations) {
        if (d.getTenantId().equals(tenantId)) {
          dataSourceConfiguration = d;
          break;
        }
      }
      
      String databaseType = dataSourceConfiguration.getDatabaseType();
      
      Environment environment = new Environment("default", transactionFactory, dataSource);
      Reader reader = new InputStreamReader(inputStream);
      Properties properties = new Properties();
      properties.put("prefix", databaseTablePrefix);
      if (databaseType != null) {
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
    
    return processEngine;
  }

  protected void createTenantSchema(String tenantId) {
    logger.info("creating/validating database schema for tenant " + tenantId);
    tenantInfoHolder.setCurrentTenantId(tenantId);
    getCommandExecutor().execute(getSchemaCommandConfig(), new ExecuteSchemaOperationCommand(multiTenantDatabaseSchemaUpdate));
  }
  
  public void addMultiTenantDataSourceConfiguration(MultiTenantDataSourceConfiguration dataSourceConfiguration) {
    
    String tenantId = dataSourceConfiguration.getTenantId();
    
    // Add datasource
    dataSourceConfigurations.add(dataSourceConfiguration);
    datasources.put(tenantId, dataSourceConfiguration.getDataSource());
    
    // Create session factory for tenant
    initSqlSessionFactoryForTenant((MultiTenantSqlSessionFactory) sqlSessionFactory, tenantId);
    
    // Register with db sql session factory
    multiTenantDbSqlSessionFactory.addDatabaseType(tenantId, dataSourceConfiguration.getDatabaseType());
    
    // Init schema
    createTenantSchema(tenantId);
  }
  
  
  // Getters and Setters ////////////////////////////////////////////////////////////////////////
  
  public List<MultiTenantDataSourceConfiguration> getDataSourceConfigurations() {
    return dataSourceConfigurations;
  }

  public TenantInfoHolder getTenantInfoHolder() {
    return tenantInfoHolder;
  }

  public void setTenantInfoHolder(TenantInfoHolder tenantInfoHolder) {
    this.tenantInfoHolder = tenantInfoHolder;
  }

  public void setDataSourceConfigurations(List<MultiTenantDataSourceConfiguration> dataSourceConfigurations) {
    this.dataSourceConfigurations = dataSourceConfigurations;
  }

}
