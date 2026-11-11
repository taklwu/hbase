/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Measures filesystem metadata operations issued while opening a region with many store files on a
 * mini HDFS cluster. Not a regular unit test (no {@code Test} prefix); run explicitly with
 * {@code -PrunLargeTests -Dtest=StoreFileOpenMetadataBenchmark}.
 * <p>
 * System properties:
 * <ul>
 * <li>{@code bench.label}: label for the results (default {@code current})</li>
 * <li>{@code bench.storeFiles}: comma separated store file counts (default {@code 10,50,100})</li>
 * <li>{@code bench.iterations}: measured region opens per store file count (default 5)</li>
 * <li>{@code bench.statLatencyMs}: latency injected into each client {@code getFileStatus} to
 * approximate a remote metadata call such as an object store HEAD (default 0)</li>
 * <li>{@code bench.output}: markdown output file (default
 * {@code target/store-file-open-benchmark-<label>.md})</li>
 * </ul>
 */
@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
public class StoreFileOpenMetadataBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileOpenMetadataBenchmark.class);

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int ROWS_PER_FLUSH = 20;

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static MiniDFSCluster dfsCluster;

  @BeforeAll
  public static void setUp() throws Exception {
    dfsCluster = UTIL.startMiniDFSCluster(1);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void benchmarkRegionOpen() throws Exception {
    String label = System.getProperty("bench.label", "current");
    int iterations = Integer.getInteger("bench.iterations", 5);
    long statLatencyMs = Long.getLong("bench.statLatencyMs", 0L);
    String output =
      System.getProperty("bench.output", "target/store-file-open-benchmark-" + label + ".md");
    List<Result> results = new ArrayList<>();
    for (String n : System.getProperty("bench.storeFiles", "10,50,100").split(",")) {
      results.add(run(Integer.parseInt(n.trim()), iterations, statLatencyMs));
    }
    String md = toMarkdown(label, iterations, statLatencyMs, results);
    LOG.info("Benchmark results:\n{}", md);
    Files.write(Paths.get(output), md.getBytes(StandardCharsets.UTF_8));
  }

  private Result run(int storeFiles, int iterations, long statLatencyMs) throws IOException {
    Configuration conf = UTIL.getConfiguration();
    Path rootDir = UTIL.getDataTestDirOnTestFS("bench-" + storeFiles);
    TableName tableName = TableName.valueOf("bench_" + storeFiles);
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();

    HRegion region = HBaseTestingUtil.createRegionAndWAL(info, rootDir, conf, htd);
    WAL wal = region.getWAL();
    for (int f = 0; f < storeFiles; f++) {
      for (int r = 0; r < ROWS_PER_FLUSH; r++) {
        Put put = new Put(Bytes.toBytes(String.format("row-%05d-%05d", f, r)));
        put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("value-" + r));
        region.put(put);
      }
      region.flush(true);
    }
    region.close();

    CountingFileSystem fs = new CountingFileSystem(rootDir.getFileSystem(conf), statLatencyMs);
    // Warm up class loading and caches; not measured.
    HRegion.openHRegion(conf, fs, rootDir, info, htd, wal).close();

    Result result = new Result(storeFiles, iterations);
    for (int i = 0; i < iterations; i++) {
      Snapshot before = Snapshot.take(dfsCluster, fs);
      long start = System.nanoTime();
      HRegion opened = HRegion.openHRegion(conf, fs, rootDir, info, htd, wal);
      long elapsedNanos = System.nanoTime() - start;
      Snapshot after = Snapshot.take(dfsCluster, fs);
      int loaded = opened.getStore(FAMILY).getStorefilesCount();
      opened.close();
      if (loaded != storeFiles) {
        throw new IllegalStateException("Expected " + storeFiles + " store files, got " + loaded);
      }
      result.add(before, after, elapsedNanos);
    }
    wal.close();
    return result;
  }

  private static String toMarkdown(String label, int iterations, long statLatencyMs,
    List<Result> results) {
    StringBuilder sb = new StringBuilder();
    sb.append("### Region open, label `").append(label).append("`\n\n");
    sb.append("Averaged over ").append(iterations).append(" opens; injected getFileStatus latency ")
      .append(statLatencyMs).append(" ms.\n\n");
    sb.append("| Store files | NN getFileInfo | NN getBlockLocations | NN total RPCs "
      + "| Client getFileStatus | Open time (ms) |\n");
    sb.append("|---:|---:|---:|---:|---:|---:|\n");
    for (Result r : results) {
      sb.append(String.format("| %d | %.1f | %.1f | %.1f | %.1f | %.1f |%n", r.storeFiles,
        r.avg(r.fileInfoOps), r.avg(r.blockLocationOps), r.avg(r.totalRpcs),
        r.avg(r.clientStatCalls), r.avg(r.openNanos) / TimeUnit.MILLISECONDS.toNanos(1)));
    }
    return sb.toString();
  }

  private static final class Snapshot {
    final long fileInfoOps;
    final long blockLocationOps;
    final long totalRpcs;
    final long clientStatCalls;

    private Snapshot(long fileInfoOps, long blockLocationOps, long totalRpcs,
      long clientStatCalls) {
      this.fileInfoOps = fileInfoOps;
      this.blockLocationOps = blockLocationOps;
      this.totalRpcs = totalRpcs;
      this.clientStatCalls = clientStatCalls;
    }

    static Snapshot take(MiniDFSCluster cluster, CountingFileSystem fs) {
      MetricsRecordBuilder nn = getMetrics("NameNodeActivity");
      MetricsRecordBuilder rpc = getMetrics("RpcActivityForPort" + cluster.getNameNodePort());
      return new Snapshot(getLongCounter("FileInfoOps", nn),
        getLongCounter("GetBlockLocations", nn), getLongCounter("RpcProcessingTimeNumOps", rpc),
        fs.statCalls.get());
    }
  }

  private static final class Result {
    final int storeFiles;
    final int iterations;
    long fileInfoOps;
    long blockLocationOps;
    long totalRpcs;
    long clientStatCalls;
    long openNanos;

    Result(int storeFiles, int iterations) {
      this.storeFiles = storeFiles;
      this.iterations = iterations;
    }

    void add(Snapshot before, Snapshot after, long elapsedNanos) {
      fileInfoOps += after.fileInfoOps - before.fileInfoOps;
      blockLocationOps += after.blockLocationOps - before.blockLocationOps;
      totalRpcs += after.totalRpcs - before.totalRpcs;
      clientStatCalls += after.clientStatCalls - before.clientStatCalls;
      openNanos += elapsedNanos;
    }

    double avg(long total) {
      return (double) total / iterations;
    }
  }

  private static final class CountingFileSystem extends FilterFileSystem {
    final AtomicLong statCalls = new AtomicLong();
    private final long statLatencyMs;

    CountingFileSystem(FileSystem fs, long statLatencyMs) {
      super(fs);
      this.statLatencyMs = statLatencyMs;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      statCalls.incrementAndGet();
      if (statLatencyMs > 0) {
        try {
          Thread.sleep(statLatencyMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException();
        }
      }
      return super.getFileStatus(f);
    }
  }
}
