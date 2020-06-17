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
package org.apache.ratis.server.simulation;

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RaftRpcMessage;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.Timestamp;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class SimulatedRequestReply<REQUEST extends RaftRpcMessage, REPLY extends RaftRpcMessage> {
  static final String SIMULATE_LATENCY_KEY
      = SimulatedRequestReply.class.getName() + ".simulateLatencyMs";
  static final int SIMULATE_LATENCY_DEFAULT
      = RaftServerConfigKeys.Rpc.TIMEOUT_MIN_DEFAULT.toIntExact(TimeUnit.MILLISECONDS);
  static final long TIMEOUT = 3000L;

  private static class ReplyOrException<REPLY> {
    private final REPLY reply;
    private final IOException ioe;

    ReplyOrException(REPLY reply, IOException ioe) {
      Preconditions.assertTrue(reply == null ^ ioe == null);
      this.reply = reply;
      this.ioe = ioe;
    }
  }

  static class EventQueue<REQUEST, REPLY> {
    private final BlockingQueue<REQUEST> requestQueue
        = new LinkedBlockingQueue<>();
    private final Map<REQUEST, ReplyOrException<REPLY>> replyMap
        = new ConcurrentHashMap<>();

    public BlockingQueue<REQUEST> getRequestQueue() {
      return requestQueue;
    }

    /** Block takeRequest for the requests sent from this server. */
    final AtomicBoolean blockTakeRequestFrom = new AtomicBoolean();
    /** Block sendRequest for the requests sent to this server. */
    final AtomicBoolean blockSendRequestTo = new AtomicBoolean();
    /** Delay takeRequest for the requests sent to this server. */
    final AtomicInteger delayTakeRequestTo = new AtomicInteger();
    /** Delay takeRequest for the requests sent from this server. */
    final AtomicInteger delayTakeRequestFrom = new AtomicInteger();

    REPLY request(REQUEST request, String serverId, Map<String, EventQueue<REQUEST, REPLY>> queues) throws InterruptedException, IOException {
      requestQueue.put(request);
      if (request instanceof RaftServerRequest) {
//        System.err.println("wangjie request 1 this:" + this.hashCode() +
//            " thread:" + Thread.currentThread().getId() + " request:" + request +
//            " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() +
//            " size:" + requestQueue.size() + " serverId:" + serverId);
      }
      synchronized (this) {
        if (request instanceof RaftServerRequest) {
//          System.err.println("wangjie request 2 this:" + this.hashCode() +
//              " thread:" + Thread.currentThread().getId() + " request:" + request +
//              " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() +
//              " size:" + requestQueue.size() + " serverId:" + serverId);
        }
        final Timestamp startTime = Timestamp.currentTime();
        while (startTime.elapsedTimeMs() < TIMEOUT &&
            !replyMap.containsKey(request)) {
          if (request instanceof RaftServerRequest) {
//            System.err.println("wangjie request 3 this:" + this.hashCode() +
//                " thread:" + Thread.currentThread().getId() + " request:" + request +
//                " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() +
//                " size:" + requestQueue.size() + " serverId:" + serverId);
          }
          this.wait(TIMEOUT); // no need to be precise here
        }
      }

      if (request instanceof RaftServerRequest) {
//        System.err.println("wangjie request 4 this:" + this.hashCode() +
//            " thread:" + Thread.currentThread().getId() + " request:" + request +
//            " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() +
//            " size:" + requestQueue.size() + " serverId:" + serverId);
      }
      if (!replyMap.containsKey(request)) {
        if (request instanceof RaftServerRequest) {
//          System.err.println("wangjie request 5 this:" + this.hashCode() +
//              " thread:" + Thread.currentThread().getId() + " request:" + request +
//              " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() +
//              " size:" + requestQueue.size() + " serverId:" + serverId);
        }
        throw new IOException("Timeout while waiting for reply of request "
            + request);
      }
      if (request instanceof RaftServerRequest) {
//        System.err.println("wangjie request 6 this:" + this.hashCode() +
//            " thread:" + Thread.currentThread().getId() + " request:" + request +
//            " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() +
//            " size:" + requestQueue.size() + " serverId:" + serverId);
      }
      final ReplyOrException<REPLY> re = replyMap.remove(request);
      if (re.ioe != null) {
        if (request instanceof RaftServerRequest) {
//          System.err.println("wangjie request 7 this:" + this.hashCode() +
//              " thread:" + Thread.currentThread().getId() + " request:" + request +
//              " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() +
//              " size:" + requestQueue.size() + " serverId:" + serverId);
        }
        throw re.ioe;
      }
      if (request instanceof RaftServerRequest) {
//        System.err.println("wangjie request 8 this:" + this.hashCode() +
//            " thread:" + Thread.currentThread().getId() + " request:" + request +
//            " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() +
//            " size:" + requestQueue.size() + " serverId:" + serverId);
      }
      return re.reply;
    }

    REQUEST takeRequest(String serverId, Map<String, EventQueue<REQUEST, REPLY>> queues) throws InterruptedException {
//      System.err.println("wangjie queue before takeRequest this:" + this.hashCode() + " thread:" + Thread.currentThread().getId() +
//          " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() + " size:" +
//          requestQueue.size() + " serverId:" + serverId);
      REQUEST request = requestQueue.take();
//      System.err.println("wangjie queue after takeRequest this:" + this.hashCode() + " thread:" + Thread.currentThread().getId() +
//           " queues:" + queues.hashCode() + " queue:" + requestQueue.hashCode() + " size:" +
//          requestQueue.size() + " serverId:" + serverId);
      return request;
    }

    void reply(REQUEST request, REPLY reply, IOException ioe)
        throws IOException {
      replyMap.put(request, new ReplyOrException<>(reply, ioe));
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  private final Map<String, EventQueue<REQUEST, REPLY>> queues
      = new ConcurrentHashMap<>();

  Map<String, EventQueue<REQUEST, REPLY>> getQueues() {
    return queues;
  }

  private final int simulateLatencyMs;

  SimulatedRequestReply(int simulateLatencyMs) {
    this.simulateLatencyMs = simulateLatencyMs;
  }

  EventQueue<REQUEST, REPLY> getQueue(String qid) {
    return queues.get(qid);
  }

  public REPLY sendRequest(REQUEST request) throws IOException {
    final String qid = request.getReplierId();
    final EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q == null) {
      throw new IOException("The peer " + qid + " is not alive.");
    }
    try {
      RaftTestUtil.block(q.blockSendRequestTo::get);
      return q.request(request, qid, queues);
    } catch (InterruptedException e) {
      throw IOUtils.toInterruptedIOException("", e);
    }
  }

  public REQUEST takeRequest(String qid, RequestHandler handler) throws IOException {
    final EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q == null) {
      throw new IOException("The RPC of " + qid + " has already shutdown.");
    }

    REQUEST request;
    try {
      // delay request for testing
      RaftTestUtil.delay(q.delayTakeRequestTo::get);
      while (true) {
        request = q.takeRequest(qid, queues);
        if (request instanceof RaftServerRequest) {
          System.err.println("wangjie takeRequest 1 this:" + this.hashCode() +
              " thread:" + Thread.currentThread().getId() + " qid:" + qid + " request:" + request.hashCode() +
              " handler:" + handler.hashCode() + " queues:" + queues.hashCode());
        }
        Preconditions.assertTrue(qid.equals(request.getReplierId()));

        // block request for testing
        final EventQueue<REQUEST, REPLY> reqQ = queues.get(request.getRequestorId());
        if (reqQ != null) {
          if (reqQ.blockTakeRequestFrom.get()) {
            continue;
          }
          RaftTestUtil.delay(reqQ.delayTakeRequestFrom::get);
        }
        break;
      }
    } catch (InterruptedException e) {
      throw IOUtils.toInterruptedIOException("", e);
    }
    if (request instanceof RaftServerRequest) {
      System.err.println("wangjie takeRequest 3 this:" + this.hashCode() +
          " thread:" + Thread.currentThread().getId() + " qid:" + qid + " request:" + request.hashCode() +
          " handler:" + handler.hashCode() + " queues:" + queues.hashCode());
    }
    return request;
  }

  public void sendReply(REQUEST request, REPLY reply, IOException ioe)
      throws IOException {
    if (reply != null) {
      Preconditions.assertTrue(
          request.getRequestorId().equals(reply.getRequestorId()));
      Preconditions.assertTrue(
          request.getReplierId().equals(reply.getReplierId()));
    }
    simulateLatency();
    final String qid = request.getReplierId();
    EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q != null) {
      q.reply(request, reply, ioe);
    }
  }

  public void shutdown(String id) {
    queues.remove(id);
  }

  public void clear() {
    queues.clear();
  }

  public void addPeer(RaftPeerId newPeer) {
    queues.put(newPeer.toString(), new EventQueue<>());
  }

  private void simulateLatency() throws IOException {
    if (simulateLatencyMs > 0) {
      int waitExpetation = simulateLatencyMs / 10;
      int waitHalfRange = waitExpetation / 3;
      int randomSleepMs = ThreadLocalRandom.current().nextInt(2 * waitHalfRange)
          + waitExpetation - waitHalfRange;
      try {
        Thread.sleep(randomSleepMs);
      } catch (InterruptedException ie) {
        throw IOUtils.toInterruptedIOException("", ie);
      }
    }
  }
}
