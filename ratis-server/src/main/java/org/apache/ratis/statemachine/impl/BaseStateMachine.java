/**
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

package org.apache.ratis.statemachine.impl;

import com.codahale.metrics.Timer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base implementation for StateMachines.
 */
public class BaseStateMachine implements StateMachine {
  private final CompletableFuture<RaftServer> server = new CompletableFuture<>();
  private volatile RaftGroupId groupId;
  private final LifeCycle lifeCycle = new LifeCycle(getClass().getSimpleName());

  private final AtomicReference<TermIndex> lastAppliedTermIndex = new AtomicReference<>();

  private final SortedMap<Long, CompletableFuture<Void>> transactionFutures = new TreeMap<>();

  public BaseStateMachine() {
    setLastAppliedTermIndex(TermIndex.newTermIndex(0, -1));
  }

  public RaftPeerId getId() {
    return server.isDone()? server.join().getId(): null;
  }

  public LifeCycle getLifeCycle() {
    return lifeCycle;
  }

  public CompletableFuture<RaftServer> getServer() {
    return server;
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  @Override
  public LifeCycle.State getLifeCycleState() {
    return lifeCycle.getCurrentState();
  }

  @Override
  public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException {
    this.groupId = raftGroupId;
    this.server.complete(raftServer);
    lifeCycle.setName("" + this);
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    return getStateMachineStorage().getLatestSnapshot();
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) throws IOException {
    // do nothing
  }

  @Override
  public void pause() {
  }

  @Override
  public void reinitialize() throws IOException {
  }

  @Override
  public TransactionContext applyTransactionSerial(TransactionContext trx) {
    return trx;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    // return the same message contained in the entry
    RaftProtos.LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry());
    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
    return CompletableFuture.completedFuture(
        Message.valueOf(trx.getLogEntry().getStateMachineLogEntry().getLogData()));
  }

  @Override
  public TermIndex getLastAppliedTermIndex() {
    return lastAppliedTermIndex.get();
  }

  public void printCallStatck(String str) {
    Throwable ex = new Throwable();
    StackTraceElement[] stackElements = ex.getStackTrace();
    if (stackElements != null) {
      for (int i = 0; i < stackElements.length; i++) {
        System.err.println(str + ": "
                + stackElements[i].getClassName()+ "|"
                + stackElements[i].getFileName()+"|"
                + stackElements[i].getLineNumber()+"|"
                + stackElements[i].getMethodName());
      }
    }
  }

  protected void setLastAppliedTermIndex(TermIndex newTI) {
    if (newTI.getIndex() == -1 || newTI.getIndex() == 0) {
      printCallStatck("setLastAppliedTermIndex this:" + this.hashCode() + " thread:" +
              Thread.currentThread().getId() + " newTI:" + newTI);
    }
    lastAppliedTermIndex.set(newTI);
  }

  @Override
  public void notifyIndexUpdate(long term, long index) {
    updateLastAppliedTermIndex(term, index);
  }

  protected boolean updateLastAppliedTermIndex(long term, long index) {
    final TermIndex newTI = TermIndex.newTermIndex(term, index);
    if (index == -1 || index == 0) {
      printCallStatck("updateLastAppliedTermIndex this:" + this.hashCode() + " thread:" +
              Thread.currentThread().getId() + " newTI:" + newTI);
    }
    final TermIndex oldTI = lastAppliedTermIndex.getAndSet(newTI);
    if (!newTI.equals(oldTI)) {
      LOG.trace("{}: update lastAppliedTermIndex from {} to {}", getId(), oldTI, newTI);
      if (oldTI != null) {
        Preconditions.assertTrue(newTI.compareTo(oldTI) >= 0,
            () -> getId() + ": Failed updateLastAppliedTermIndex: newTI = "
                + newTI + " < oldTI = " + oldTI);
      }
      return true;
    }

    synchronized (transactionFutures) {
      for(long i; !transactionFutures.isEmpty() && (i = transactionFutures.firstKey()) <= index; ) {
        transactionFutures.remove(i).complete(null);
      }
    }
    return false;
  }

  @Override
  public long takeSnapshot() throws IOException {
    return RaftServerConstants.INVALID_LOG_INDEX;
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return new StateMachineStorage() {
      @Override
      public void init(RaftStorage raftStorage) throws IOException {
      }

      @Override
      public SnapshotInfo getLatestSnapshot() {
        return null;
      }

      @Override
      public void format() throws IOException {
      }

      @Override
      public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) {
      }
    };
  }

  @Override
  public CompletableFuture<Message> queryStale(Message request, long minIndex) {
    if (getLastAppliedTermIndex().getIndex() < minIndex) {
      synchronized (transactionFutures) {
        if (getLastAppliedTermIndex().getIndex() < minIndex) {
          return transactionFutures.computeIfAbsent(minIndex, key -> new CompletableFuture<>())
              .thenCompose(v -> query(request));
        }
      }
    }
    return query(request);
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
    return TransactionContext.newBuilder()
        .setStateMachine(this)
        .setClientRequest(request)
        .build();
  }

  @Override
  public TransactionContext cancelTransaction(TransactionContext trx) throws IOException {
    return trx;
  }

  @Override
  public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
    return trx;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":"
        + (!server.isDone()? "uninitialized": getId() + ":" + groupId);
  }


  protected CompletableFuture<Message> recordTime(Timer timer, Task task) {
    final Timer.Context timerContext = timer.time();
    try {
      return task.run();
    } finally {
      timerContext.stop();
    }
  }

  protected interface Task {
    CompletableFuture<Message> run();
  }

}
