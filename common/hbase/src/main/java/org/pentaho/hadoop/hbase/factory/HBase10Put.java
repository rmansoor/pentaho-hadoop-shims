/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/
package org.pentaho.hadoop.hbase.factory;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.access.Permission;
import org.pentaho.hbase.factory.HBasePut;

import java.util.HashMap;
import java.util.Map;

public class HBase10Put implements HBasePut {
  Put put;

  HBase10Put( byte[] key ) {
    this.put = new Put( key );
  }
  
  @Override
  public void setWriteToWAL( boolean writeToWAL ) {
    put.setDurability( writeToWAL ? Durability.USE_DEFAULT : Durability.SKIP_WAL );
  }

  @Override
  public void addColumn( byte[] colFamily, byte[] colName, byte[] colValue ) {
    put.addColumn( colFamily, colName, colValue );
  }

  @Override
  public void addColumn( byte[] colFamily, byte[] colName, long timestamp, byte[] colValue ) {
    put.addColumn( colFamily, colName, timestamp, colValue );
  }

  @Override
  public void setAcl( Map<String, String[]> userPermissions ) {
    put.setACL( convert( userPermissions ) );
  }

  Put getPut() {
    return put;
  }

  private Map<String, Permission> convert( Map<String, String[]> userPermissions ) {
    Map<String, Permission> permissionMap = new HashMap<String, Permission>(  );
    for ( String user :userPermissions.keySet() ) {
      String[] permissions  = userPermissions.get( user );
      Permission.Action[] actions = new Permission.Action[userPermissions.size()];
      int i = 0;
      for( String permission : permissions ) {
        switch ( permission.toUpperCase() ) {
          case "WRITE":
            actions[ i++] = Permission.Action.WRITE;
          case "READ":
            actions[ i++] = Permission.Action.READ;
          case "EXECUTE":
            actions[ i++] = Permission.Action.EXEC;
          case "CREATE":
            actions[ i++] = Permission.Action.CREATE;
        }
      }
      permissionMap.put( user, new Permission( actions ) );
    }
    return permissionMap;
  }
}
