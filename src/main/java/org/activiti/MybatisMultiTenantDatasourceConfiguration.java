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

import org.activiti.engine.ActivitiException;
import org.activiti.engine.impl.util.ReflectUtil;
import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Joram Barrez
 */
public class MybatisMultiTenantDatasourceConfiguration implements MultiTenantDataSourceConfiguration {
  
  private static final Logger logger = LoggerFactory.getLogger(MybatisMultiTenantDatasourceConfiguration.class);
  
  protected String tenantId;
  
  protected String databaseType;
  
  protected String jdbcUrl;
  protected String jdbcUsername;
  protected String jdbcPassword;
  protected String jdbcDriver;
  
  protected int jdbcMaxActiveConnections;
  protected int jdbcMaxIdleConnections;
  protected int jdbcMaxCheckoutTime;
  protected int jdbcMaxWaitTime;
  protected boolean jdbcPingEnabled;
  protected String jdbcPingQuery;
  protected int jdbcPingConnectionNotUsedFor;
  protected int jdbcDefaultTransactionIsolationLevel;
  
  public MybatisMultiTenantDatasourceConfiguration(String tenantId, String databaseType, String jdbcUrl, String jdbcUsername, String jdbcPassword, String jdbcDriver) {
    this.tenantId = tenantId;
    this.databaseType = databaseType;
    this.jdbcUrl = jdbcUrl;
    this.jdbcUsername = jdbcUsername;
    this.jdbcPassword = jdbcPassword;
    this.jdbcDriver = jdbcDriver;
  }
  
  public DataSource getDataSource() {
    logger.info("Creating datasource for tenant " + tenantId + " at jdbc url " + jdbcUrl);
    
    DataSource dataSource = null;
    if (jdbcUrl != null) {

      if ((jdbcDriver == null) || (jdbcUsername == null)) {
        throw new ActivitiException("DataSource or JDBC properties have to be specified in a process engine configuration");
      }

      PooledDataSource pooledDataSource = new PooledDataSource(ReflectUtil.getClassLoader(), jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword);

      if (jdbcMaxActiveConnections > 0) {
        pooledDataSource.setPoolMaximumActiveConnections(jdbcMaxActiveConnections);
      }
      if (jdbcMaxIdleConnections > 0) {
        pooledDataSource.setPoolMaximumIdleConnections(jdbcMaxIdleConnections);
      }
      if (jdbcMaxCheckoutTime > 0) {
        pooledDataSource.setPoolMaximumCheckoutTime(jdbcMaxCheckoutTime);
      }
      if (jdbcMaxWaitTime > 0) {
        pooledDataSource.setPoolTimeToWait(jdbcMaxWaitTime);
      }
      if (jdbcPingEnabled == true) {
        pooledDataSource.setPoolPingEnabled(true);
        if (jdbcPingQuery != null) {
          pooledDataSource.setPoolPingQuery(jdbcPingQuery);
        }
        pooledDataSource.setPoolPingConnectionsNotUsedFor(jdbcPingConnectionNotUsedFor);
      }
      if (jdbcDefaultTransactionIsolationLevel > 0) {
        pooledDataSource.setDefaultTransactionIsolationLevel(jdbcDefaultTransactionIsolationLevel);
      }
      
      dataSource = pooledDataSource;
    }

    if (dataSource instanceof PooledDataSource) {
      // ACT-233: connection pool of Ibatis is not properly
      // initialized if this is not called!
      ((PooledDataSource) dataSource).forceCloseAll();
    }
    
    return dataSource;
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getDatabaseType() {
    return databaseType;
  }

  public void setDatabaseType(String databaseType) {
    this.databaseType = databaseType;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getJdbcUsername() {
    return jdbcUsername;
  }

  public void setJdbcUsername(String jdbcUsername) {
    this.jdbcUsername = jdbcUsername;
  }

  public String getJdbcPassword() {
    return jdbcPassword;
  }

  public void setJdbcPassword(String jdbcPassword) {
    this.jdbcPassword = jdbcPassword;
  }

  public String getJdbcDriver() {
    return jdbcDriver;
  }

  public void setJdbcDriver(String jdbcDriver) {
    this.jdbcDriver = jdbcDriver;
  }

  public int getJdbcMaxActiveConnections() {
    return jdbcMaxActiveConnections;
  }

  public void setJdbcMaxActiveConnections(int jdbcMaxActiveConnections) {
    this.jdbcMaxActiveConnections = jdbcMaxActiveConnections;
  }

  public int getJdbcMaxIdleConnections() {
    return jdbcMaxIdleConnections;
  }

  public void setJdbcMaxIdleConnections(int jdbcMaxIdleConnections) {
    this.jdbcMaxIdleConnections = jdbcMaxIdleConnections;
  }

  public int getJdbcMaxCheckoutTime() {
    return jdbcMaxCheckoutTime;
  }

  public void setJdbcMaxCheckoutTime(int jdbcMaxCheckoutTime) {
    this.jdbcMaxCheckoutTime = jdbcMaxCheckoutTime;
  }

  public int getJdbcMaxWaitTime() {
    return jdbcMaxWaitTime;
  }

  public void setJdbcMaxWaitTime(int jdbcMaxWaitTime) {
    this.jdbcMaxWaitTime = jdbcMaxWaitTime;
  }

  public boolean isJdbcPingEnabled() {
    return jdbcPingEnabled;
  }

  public void setJdbcPingEnabled(boolean jdbcPingEnabled) {
    this.jdbcPingEnabled = jdbcPingEnabled;
  }

  public String getJdbcPingQuery() {
    return jdbcPingQuery;
  }

  public void setJdbcPingQuery(String jdbcPingQuery) {
    this.jdbcPingQuery = jdbcPingQuery;
  }

  public int getJdbcPingConnectionNotUsedFor() {
    return jdbcPingConnectionNotUsedFor;
  }

  public void setJdbcPingConnectionNotUsedFor(int jdbcPingConnectionNotUsedFor) {
    this.jdbcPingConnectionNotUsedFor = jdbcPingConnectionNotUsedFor;
  }

  public int getJdbcDefaultTransactionIsolationLevel() {
    return jdbcDefaultTransactionIsolationLevel;
  }

  public void setJdbcDefaultTransactionIsolationLevel(int jdbcDefaultTransactionIsolationLevel) {
    this.jdbcDefaultTransactionIsolationLevel = jdbcDefaultTransactionIsolationLevel;
  }
  
}
