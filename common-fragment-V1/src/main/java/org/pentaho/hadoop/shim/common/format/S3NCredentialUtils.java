/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format;

import com.amazonaws.auth.AWSCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.pentaho.amazon.s3.PentahoDefaultAWSCredentialsProviderChain;
import org.pentaho.amazon.s3.PentahoVFSAwareCredentials;
import org.pentaho.amazon.s3.PentahoVFSAwareCredentialsProvider;

import java.net.URI;

public class S3NCredentialUtils {

  private static final String S3NSCHEME = "s3n";
  private static final String S3ASCHEME = "s3a";
  private static final String S3NROOTBUCKET = S3NSCHEME + "/";

  private static boolean s3nIsSupported = true;

  public static void applyS3CredentialsToHadoopConfigurationIfNecessary( String filename, Configuration conf ) {
    AWSCredentials credentials; Path outputFile = new Path( scrubFilePathIfNecessary( filename ) );
    URI uri = outputFile.toUri();
    String scheme = uri != null ? uri.getScheme() : null; if ( scheme != null && scheme.equals( S3NSCHEME ) ) {
      credentials = PentahoDefaultAWSCredentialsProviderChain.getInstance().getCredentials();
      if ( credentials != null ) {
        conf.set( "fs.s3n.awsAccessKeyId", credentials.getAWSAccessKeyId() );
        conf.set( "fs.s3n.awsSecretAccessKey", credentials.getAWSSecretKey() );
        conf.set( "fs.s3.buffer.dir", System.getProperty( "java.io.tmpdir" ) );
      }
    } else if ( scheme != null && scheme.equals( S3ASCHEME ) ) {
      PentahoVFSAwareCredentialsProvider credentialsProvider = new PentahoVFSAwareCredentialsProvider();
      credentials = credentialsProvider.getCredentials();
      if ( credentials != null && credentials instanceof PentahoVFSAwareCredentials ) {
        PentahoVFSAwareCredentials vfsAwareCredentials = (PentahoVFSAwareCredentials) credentials;
        conf.set( "fs.s3a.path.style.access", Boolean.toString( vfsAwareCredentials.getPathStyleAccess() ) );
        conf.set( "fs.s3a.secret.key", vfsAwareCredentials.getAWSSecretKey() );
        conf.set( "fs.s3a.access.key", vfsAwareCredentials.getAWSAccessKeyId() );
        conf.set( "fs.s3a.endpoint", vfsAwareCredentials.getEndpointUrl() );
      }
    }
  }

  public static String scrubFilePathIfNecessary( String filename ) {
    if ( filename != null ) {
      filename = filename.replace( S3NROOTBUCKET, "" ); if ( !s3nIsSupported ) {
        filename = filename.replace( S3NSCHEME, S3ASCHEME );
      }
    } return filename;
  }

  public static boolean isS3nIsSupported() {
    return s3nIsSupported;
  }

  public static void setS3nIsSupported( boolean supported ) {
    s3nIsSupported = supported;
  }
}
