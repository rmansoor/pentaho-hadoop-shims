# Hadoop FileSystem Registration Without ServiceLoader

## Overview

This document describes how to register Hadoop FileSystem implementations in **non-OSGi environments** without relying on Java's ServiceLoader mechanism or `META-INF/services` files.

## The Problem

The traditional approach uses `META-INF/services/org.apache.hadoop.fs.FileSystem` with entries like:

```
org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge
org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem
org.apache.hadoop.fs.adl.AdlFileSystem
```

**Limitations:**
- ❌ ServiceLoader requires proper classloader visibility
- ❌ Doesn't work well in OSGi/Karaf environments
- ❌ Static registration, no runtime flexibility
- ❌ Can cause issues in complex classloader hierarchies
- ❌ Doesn't work in some containerized environments

## The Solution: FileSystemRegistry

We've created `FileSystemRegistry` - a static registry that programmatically configures Hadoop's Configuration objects with FileSystem implementations.

### How It Works

Instead of ServiceLoader discovering implementations, we:
1. Register FileSystem implementations in a static registry
2. Apply them to Hadoop Configuration objects via `fs.<scheme>.impl` properties
3. Hadoop uses these properties when `FileSystem.get()` is called

```java
// Hadoop internally does this:
String impl = conf.get("fs.pvfs.impl");
// Returns: "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge"
```

## Quick Start

### Option 1: Use Defaults (Simplest)

```java
import org.pentaho.hadoop.shim.common.fs.FileSystemRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

// At application startup
public static void main(String[] args) {
    // Auto-registers: pvfs, adl, wasb, abfss, s3a, gs
    FileSystemRegistry.registerDefaults();
    
    // Create configuration with registered implementations
    Configuration conf = FileSystemRegistry.createConfiguration();
    
    // Use FileSystem normally
    FileSystem fs = FileSystem.get(uri, conf);
}
```

### Option 2: Register Custom Implementations

```java
// Register specific implementations
FileSystemRegistry.registerFileSystem("pvfs", "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge");
FileSystemRegistry.registerFileSystem("s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem");
FileSystemRegistry.registerFileSystem("adl", "org.apache.hadoop.fs.adl.AdlFileSystem");

// For Hadoop 2.x+ AbstractFileSystem API
FileSystemRegistry.registerAbstractFileSystem("adl", "org.apache.hadoop.fs.adl.Adl");

Configuration conf = FileSystemRegistry.createConfiguration();
```

### Option 3: Apply to Existing Configuration

```java
// You have an existing Configuration
Configuration conf = new Configuration();
conf.set("my.custom.property", "value");

// Add FileSystem registrations to it
FileSystemRegistry.applyToConfiguration(conf);

// Now it has both your settings and FileSystem implementations
```

## Use Cases

### Standalone Hadoop Application

```java
public class MyHadoopApp {
    public static void main(String[] args) throws IOException {
        // Initialize at startup
        FileSystemRegistry.registerDefaults();
        
        Configuration conf = FileSystemRegistry.createConfiguration();
        conf.set("fs.defaultFS", "hdfs://namenode:9000");
        
        FileSystem fs = FileSystem.get(conf);
        // Use filesystem...
    }
}
```

### Spark Application

```java
public class MySparkJob {
    public static void main(String[] args) {
        // Register before creating SparkContext
        FileSystemRegistry.registerDefaults();
        
        SparkConf sparkConf = new SparkConf()
            .setAppName("My Spark Job");
        
        // Apply to Spark's Hadoop configuration
        Configuration hadoopConf = FileSystemRegistry.createConfiguration();
        sparkConf.set("spark.hadoop.fs.pvfs.impl", 
            hadoopConf.get("fs.pvfs.impl"));
        
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // Now you can use custom filesystems in Spark
        JavaRDD<String> data = sc.textFile("pvfs://conn/bucket/data.txt");
    }
}
```

### MapReduce Job

```java
public class MyMapReduceJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = FileSystemRegistry.createConfiguration();
        
        Job job = Job.getInstance(conf, "My MR Job");
        job.setJarByClass(MyMapReduceJob.class);
        
        FileInputFormat.addInputPath(job, 
            new Path("pvfs://connection/bucket/input"));
        FileOutputFormat.setOutputPath(job, 
            new Path("pvfs://connection/bucket/output"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Testing

```java
@Before
public void setup() {
    // Register test FileSystem implementations
    FileSystemRegistry.clearAll();
    FileSystemRegistry.registerFileSystem("test", 
        "com.example.TestFileSystem");
}

@Test
public void testFileSystemOperations() throws IOException {
    Configuration conf = FileSystemRegistry.createConfiguration();
    FileSystem fs = FileSystem.get(URI.create("test://bucket"), conf);
    // Test operations...
}

@After
public void teardown() {
    FileSystemRegistry.clearAll();
}
```

## Migration Guide

### From META-INF/services to FileSystemRegistry

**Before:**
```
# File: META-INF/services/org.apache.hadoop.fs.FileSystem
org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge
org.apache.hadoop.fs.adl.AdlFileSystem
```

**After:**
```java
// In application startup code
FileSystemRegistry.registerFileSystem("pvfs", 
    "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge");
FileSystemRegistry.registerFileSystem("adl", 
    "org.apache.hadoop.fs.adl.AdlFileSystem");
```

### Steps:

1. **Remove** `META-INF/services/org.apache.hadoop.fs.FileSystem` file
2. **Add** `FileSystemRegistry` initialization to your application startup
3. **Use** `FileSystemRegistry.createConfiguration()` or `applyToConfiguration()`
4. **Test** that FileSystem.get() works with your custom schemes

## API Reference

### Registration Methods

```java
// Register a FileSystem implementation
FileSystemRegistry.registerFileSystem(String scheme, String className);

// Register AbstractFileSystem (Hadoop 2.x+)
FileSystemRegistry.registerAbstractFileSystem(String scheme, String className);

// Register only if class is available (no ClassNotFoundException)
FileSystemRegistry.registerFileSystemIfAvailable(String scheme, String className);

// Unregister a scheme
FileSystemRegistry.unregisterFileSystem(String scheme);

// Clear all registrations
FileSystemRegistry.clearAll();

// Register all default Pentaho implementations
FileSystemRegistry.registerDefaults();
```

### Configuration Methods

```java
// Create new Configuration with registrations applied
Configuration conf = FileSystemRegistry.createConfiguration();

// Create Configuration based on existing one
Configuration conf = FileSystemRegistry.createConfiguration(baseConf);

// Apply to existing Configuration
FileSystemRegistry.applyToConfiguration(conf);
```

### Inspection Methods

```java
// Check if scheme is registered
boolean registered = FileSystemRegistry.isRegistered("pvfs");

// Get implementation class for scheme
String impl = FileSystemRegistry.getImplementation("pvfs");

// Get all registrations
Map<String, String> all = FileSystemRegistry.getAllRegistrations();
```

## Default Registrations

When you call `FileSystemRegistry.registerDefaults()`, these implementations are registered:

| Scheme | Implementation | Description |
|--------|---------------|-------------|
| `pvfs` | `org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge` | Pentaho VFS Bridge |
| `adl` | `org.apache.hadoop.fs.adl.AdlFileSystem` | Azure Data Lake Gen1 |
| `wasb` | `org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem` | Azure Blob Storage |
| `abfss` | `org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem` | Azure Blob Storage (Secure) |
| `s3a` | `org.apache.hadoop.fs.s3a.S3AFileSystem` | Amazon S3 (if available) |
| `gs` | `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem` | Google Cloud Storage (if available) |

Implementations marked "if available" are only registered if the class is on the classpath.

## Comparison: ServiceLoader vs FileSystemRegistry

| Feature | ServiceLoader | FileSystemRegistry |
|---------|--------------|-------------------|
| **OSGi Compatible** | ⚠️ Limited | ✅ Yes |
| **Non-OSGi Compatible** | ✅ Yes | ✅ Yes |
| **Runtime Registration** | ❌ No | ✅ Yes |
| **Conditional Loading** | ❌ No | ✅ Yes |
| **Classpath Isolation** | ⚠️ Issues | ✅ Works |
| **Testing Flexibility** | ❌ Limited | ✅ Full control |
| **Configuration** | ❌ None | ✅ Per-instance |

## Troubleshooting

### Issue: FileSystem not found

```
java.io.IOException: No FileSystem for scheme: pvfs
```

**Solution:**
```java
// Ensure FileSystemRegistry is initialized
FileSystemRegistry.registerDefaults();

// Ensure Configuration has registrations applied
Configuration conf = FileSystemRegistry.createConfiguration();
FileSystem fs = FileSystem.get(uri, conf);
```

### Issue: Wrong FileSystem implementation used

**Solution:**
```java
// Check what's registered
String impl = FileSystemRegistry.getImplementation("pvfs");
System.out.println("Registered: " + impl);

// Re-register if needed
FileSystemRegistry.registerFileSystem("pvfs", "correct.implementation.Class");
```

### Issue: ClassNotFoundException

**Solution:**
```java
// Use conditional registration
FileSystemRegistry.registerFileSystemIfAvailable("gs", 
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

// This won't throw exception if class not found
```

## Best Practices

1. **Initialize Early**: Call `registerDefaults()` in your application's main method or initialization code
2. **Use createConfiguration()**: Prefer creating new configurations rather than modifying shared ones
3. **Conditional Registration**: Use `registerFileSystemIfAvailable()` for optional dependencies
4. **Testing**: Clear registrations between tests with `clearAll()`
5. **Thread Safety**: All methods are synchronized, safe for concurrent use
6. **Logging**: Enable DEBUG logging to see registration activity

## Performance

- **Overhead**: Minimal - just HashMap lookups and Configuration.set() calls
- **Memory**: Negligible - stores only scheme->class name mappings
- **Thread-Safe**: All methods synchronized
- **Initialization**: Static block runs once on class load

## Examples

See `FileSystemRegistryExample.java` for complete working examples including:
- Basic usage
- Custom registration
- Spark jobs
- MapReduce jobs
- Testing patterns

## Conclusion

`FileSystemRegistry` provides a robust, flexible alternative to ServiceLoader that works in all environments including non-OSGi, OSGi, standalone applications, Spark, MapReduce, and testing scenarios.

**Recommended Approach:**
- Use `FileSystemRegistry.registerDefaults()` for most cases
- Remove `META-INF/services` files
- Initialize in application startup code
- Use `createConfiguration()` when getting FileSystem instances
