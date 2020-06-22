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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.statemachine.RaftSnapshotBaseTest;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public abstract class InstallSnapshotNotificationTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static final Logger LOG = LoggerFactory.getLogger(InstallSnapshotNotificationTests.class);

  {
    Log4jUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
  }

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        StateMachine4InstallSnapshotNotificationTests.class, StateMachine.class);
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(prop, false);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(
        prop, SNAPSHOT_TRIGGER_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, true);

    RaftServerConfigKeys.Log.setPurgeGap(prop, PURGE_GAP);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf(1024)); // 1k segment
  }

  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 64;
  private static final int PURGE_GAP = 8;
  private static final AtomicReference<SnapshotInfo> leaderSnapshotInfoRef = new AtomicReference<>();

  private static class StateMachine4InstallSnapshotNotificationTests extends SimpleStateMachine4Testing {
    @Override
    public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
        RaftProtos.RoleInfoProto roleInfoProto,
        TermIndex termIndex) {
      final SingleFileSnapshotInfo leaderSnapshotInfo = (SingleFileSnapshotInfo) leaderSnapshotInfoRef.get();
      LOG.info("{}: leaderSnapshotInfo = {}", getId(), leaderSnapshotInfo);
      if (leaderSnapshotInfo == null) {
        return super.notifyInstallSnapshotFromLeader(roleInfoProto, termIndex);
      }

      try {
        Path leaderSnapshotFile = leaderSnapshotInfo.getFile().getPath();
        File followerSnapshotFilePath = new File(getSMdir(),
            leaderSnapshotFile.getFileName().toString());
        Files.copy(leaderSnapshotFile, followerSnapshotFilePath.toPath());
      } catch (IOException e) {
        LOG.error("Failed notifyInstallSnapshotFromLeader", e);
        return JavaUtils.completeExceptionally(e);
      }
      return CompletableFuture.completedFuture(leaderSnapshotInfo.getTermIndex());
    }
  }

  /**
   * Basic test for install snapshot notification: start a one node cluster
   * (disable install snapshot option) and let it generate a snapshot. Then
   * delete the log and restart the node, and add more nodes as followers.
   * The new follower nodes should get a install snapshot notification.
   */
  @Test
  public void testAddNewFollowers() throws Exception {
    System.err.println("wangjie testAddNewFollowers");
    runWithNewCluster(1, this::testAddNewFollowers);
  }

  private void testAddNewFollowers(CLUSTER cluster) throws Exception {
    leaderSnapshotInfoRef.set(null);
    final List<RaftStorageDirectory.LogPathAndIndex> logs;
    int i = 0;
    try {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try(final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply
              reply = client.send(new RaftTestUtil.SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }
      }

      // wait for the snapshot to be done
      RaftStorageDirectory storageDirectory = cluster.getLeader().getState()
          .getStorage().getStorageDir();

      final long nextIndex = cluster.getLeader().getState().getLog().getNextIndex();
      LOG.info("nextIndex = {}", nextIndex);
      final List<File> snapshotFiles = RaftSnapshotBaseTest.getSnapshotFiles(cluster,
          nextIndex - SNAPSHOT_TRIGGER_THRESHOLD, nextIndex);
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(snapshotFiles.stream().anyMatch(RaftSnapshotBaseTest::exists));
        return null;
      }, 10, ONE_SECOND, "snapshotFile.exist", LOG);
      logs = storageDirectory.getLogSegmentFiles();
    } finally {
      cluster.shutdown();
    }

    // delete the log segments from the leader
    LOG.info("Delete logs {}", logs);
    for (RaftStorageDirectory.LogPathAndIndex path : logs) {
      FileUtils.deleteFully(path.getPath()); // the log may be already puged
    }

    // restart the peer
    LOG.info("Restarting the cluster");
    cluster.restart(false);
    try {
      RaftSnapshotBaseTest.assertLeaderContent(cluster);

      // generate some more traffic
      try(final RaftClient client = cluster.createClient(cluster.getLeader().getId())) {
        Assert.assertTrue(client.send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
      }

      final SnapshotInfo leaderSnapshotInfo = cluster.getLeader().getStateMachine().getLatestSnapshot();
      final boolean set = leaderSnapshotInfoRef.compareAndSet(null, leaderSnapshotInfo);
      Assert.assertTrue(set);

      // add two more peers
      final MiniRaftCluster.PeerChanges change = cluster.addNewPeers(2, true);
      // trigger setConfiguration
      cluster.setConfiguration(change.allPeersInNewConf);

      RaftServerTestUtil
          .waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);
    } finally {
      cluster.shutdown();
    }
  }
}