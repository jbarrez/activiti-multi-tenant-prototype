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
package org.activiti.tenant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Joram Barrez
 */
public class TenantComponent {
  
  public static Map<String, List<String>> TENANT_MAPPING = new HashMap<String, List<String>>();
  
  public static Map<String, String> USER_TO_TENANT_MAPPING = new HashMap<String, String>();
  
  static {
    
    TENANT_MAPPING.put("Alfresco", new ArrayList<String>());
    TENANT_MAPPING.get("Alfresco").add("joram");
    TENANT_MAPPING.get("Alfresco").add("tijs");
    TENANT_MAPPING.get("Alfresco").add("paul");
    
    TENANT_MAPPING.put("Uni", new ArrayList<String>());
    TENANT_MAPPING.get("Uni").add("raphael");
    TENANT_MAPPING.get("Uni").add("john");
    
    
    for (String tenantId : TENANT_MAPPING.keySet()) {
      List<String> userIds = TENANT_MAPPING.get(tenantId);
      for (String userId : userIds) {
        USER_TO_TENANT_MAPPING.put(userId, tenantId);
      }
    }
    
  }
  
}
