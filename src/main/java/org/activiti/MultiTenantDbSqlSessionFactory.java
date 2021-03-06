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

import java.util.HashMap;
import java.util.Map;

import org.activiti.engine.impl.db.DbSqlSessionFactory;
import org.activiti.tenant.TenantInfoHolder;

/**
 * @author Joram Barrez
 */
public class MultiTenantDbSqlSessionFactory extends DbSqlSessionFactory {
  
  protected TenantInfoHolder tenantInfoHolder;
  
  protected Map<String, String> tenantToDatabaseTypeMap = new HashMap<String, String>();
  
  public MultiTenantDbSqlSessionFactory(TenantInfoHolder tenantInfoHolder) {
    this.tenantInfoHolder = tenantInfoHolder;
  }
  
  public void addDatabaseType(String tenantId, String databaseType) {
    tenantToDatabaseTypeMap.put(tenantId, databaseType);
  }
  
  @Override
  public String getDatabaseType() {
    return tenantToDatabaseTypeMap.get(tenantInfoHolder.getCurrentTenantId());
  }

}
