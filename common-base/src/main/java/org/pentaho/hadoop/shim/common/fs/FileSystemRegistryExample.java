/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Example usage of FileSystemRegistry for non-OSGi environments.
 * 
 * This demonstrates how to register and use custom FileSystem implementations
 * without relying on Java ServiceLoader or META-INF/services files.
 */
public class FileSystemRegistryExample {
  
  /**
   * Example 1: Basic usage with auto-registration of defaults
   */
  public static void exampleBasicUsage() throws IOException {
    // FileSystemRegistry automatically registers defaults on first use
    
    // Create a configuration with registered implementations
    Configuration conf = FileSystemRegistry.createConfiguration();
    
    // Now you can use FileSystem.get() with custom schemes
    FileSystem fs = FileSystem.get( URI.create( "pvfs://myconnection/mybucket/myfile.txt" ), conf );
    
    // Use the filesystem
    Path path = new Path( "pvfs://myconnection/mybucket/myfile.txt" );
    if ( fs.exists( path ) ) {
      System.out.println( "File exists!" );
    }
  }
  
  /**
   * Example 2: Register additional custom FileSystem implementations
   */
  public static void exampleCustomRegistration() throws IOException {
    // Register custom implementations
    FileSystemRegistry.registerFileSystem( "myfs", "com.example.MyCustomFileSystem" );
    FileSystemRegistry.registerFileSystem( "s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem" );
    
    // Create configuration with all registered implementations
    Configuration conf = FileSystemRegistry.createConfiguration();
    
    // Now custom schemes will work
    FileSystem fs = FileSystem.get( URI.create( "myfs://bucket/path" ), conf );
  }
  
  /**
   * Example 3: Apply to existing Configuration
   */
  public static void exampleApplyToExisting() throws IOException {
    // Start with an existing configuration
    Configuration conf = new Configuration();
    conf.set( "some.existing.property", "value" );
    
    // Add FileSystem registrations to it
    FileSystemRegistry.applyToConfiguration( conf );
    
    // Now it has both existing settings and FileSystem registrations
    FileSystem fs = FileSystem.get( URI.create( "adl://myaccount.azuredatalakestore.net/path" ), conf );
  }
  
  /**
   * Example 4: Spark Job with FileSystemRegistry
   */
  public static void exampleSparkJob() {
    // In your Spark application main method or driver:
    
    // Register FileSystems before creating SparkContext
    FileSystemRegistry.registerDefaults();
    
    // When Spark tasks run, ensure Configuration is set up:
    Configuration conf = FileSystemRegistry.createConfiguration();
    
    // Pass this configuration to Spark operations
    // conf.set("spark.hadoop.fs.pvfs.impl", "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge");
    // ... etc
  }
  
  /**
   * Example 5: MapReduce Job with FileSystemRegistry
   */
  public static void exampleMapReduceJob() throws IOException {
    // In your MapReduce driver:
    
    Configuration conf = FileSystemRegistry.createConfiguration();
    
    // Add other MR configuration
    conf.set( "mapreduce.job.reduces", "10" );
    
    // Use with Job
    // Job job = Job.getInstance(conf, "My Job");
    // FileInputFormat.addInputPath(job, new Path("pvfs://conn/bucket/input"));
  }
  
  /**
   * Example 6: Check what's registered
   */
  public static void exampleInspectRegistrations() {
    // Check if a scheme is registered
    if ( FileSystemRegistry.isRegistered( "pvfs" ) ) {
      String impl = FileSystemRegistry.getImplementation( "pvfs" );
      System.out.println( "PVFS implementation: " + impl );
    }
    
    // Get all registrations
    FileSystemRegistry.getAllRegistrations().forEach( ( scheme, impl ) -> {
      System.out.println( scheme + " -> " + impl );
    } );
  }
  
  /**
   * Example 7: Application startup initialization
   */
  public static void initializeApplication() {
    // Call this at application startup (main method, servlet init, etc.)
    
    // Option 1: Use defaults only
    FileSystemRegistry.registerDefaults();
    
    // Option 2: Add custom implementations
    FileSystemRegistry.registerDefaults();
    FileSystemRegistry.registerFileSystem( "custom", "com.mycompany.CustomFileSystem" );
    
    // Option 3: Clear defaults and register only what you need
    FileSystemRegistry.clearAll();
    FileSystemRegistry.registerFileSystem( "pvfs", "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge" );
    FileSystemRegistry.registerFileSystem( "s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem" );
  }
  
  /**
   * Example 8: Conditional registration based on classpath
   */
  public static void exampleConditionalRegistration() {
    // Register only if classes are available (no ClassNotFoundException)
    FileSystemRegistry.registerFileSystemIfAvailable( "gs", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" );
    FileSystemRegistry.registerFileSystemIfAvailable( "oss", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem" );
    
    // These will only be registered if the respective libraries are on the classpath
  }
}
