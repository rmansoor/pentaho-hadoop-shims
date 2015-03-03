/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common;

import static org.junit.Assert.*;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.tools.grunt.GruntParser;
import org.junit.Test;
import org.pentaho.hadoop.shim.spi.PigShim.ExecutionMode;

import java.io.StringReader;
import java.util.Properties;

public class CommonPigShimTest {

  class TestPigShim extends CommonPigShim {
    @Override public int[] executeScript( String pigScript, ExecutionMode executionMode, Properties properties )
      throws Exception {
      return new int[0];
    }
  }

  @Test
  public void isLocalExecutionSupported() {
    assertTrue( new TestPigShim().isLocalExecutionSupported() );
  }

  @Test
  public void getExecType_local() {
    assertEquals( ExecType.LOCAL, new TestPigShim().getExecType( ExecutionMode.LOCAL ) );
  }

  @Test
  public void getExecType_mapreduce() {
    assertEquals( ExecType.MAPREDUCE, new TestPigShim().getExecType( ExecutionMode.MAPREDUCE ) );
  }

}
