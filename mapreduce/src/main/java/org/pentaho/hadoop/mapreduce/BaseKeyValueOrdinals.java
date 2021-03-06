/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.mapreduce;

import org.pentaho.di.core.row.RowMetaInterface;

public abstract class BaseKeyValueOrdinals {
  private int keyOrdinal = -1;
  private int valueOrdinal = -1;

  public BaseKeyValueOrdinals( RowMetaInterface rowMeta ) {
    if ( rowMeta != null ) {
      String[] fieldNames = rowMeta.getFieldNames();

      for ( int i = 0; i < fieldNames.length; i++ ) {
        if ( fieldNames[ i ].equalsIgnoreCase( getKeyName() ) ) {
          keyOrdinal = i;
          if ( valueOrdinal >= 0 ) {
            break;
          }
        } else if ( fieldNames[ i ].equalsIgnoreCase( getValueName() ) ) {
          valueOrdinal = i;
          if ( keyOrdinal >= 0 ) {
            break;
          }
        }
      }
    }
  }

  protected abstract String getKeyName();

  protected abstract String getValueName();

  public int getKeyOrdinal() {
    return keyOrdinal;
  }

  public int getValueOrdinal() {
    return valueOrdinal;
  }
}
