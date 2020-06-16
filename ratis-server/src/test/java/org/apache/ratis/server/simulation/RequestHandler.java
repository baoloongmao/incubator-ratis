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
package org.apache.ratis.server.simulation;

import org.apache.ratis.protocol.RaftRpcMessage;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class RequestHandler<REQUEST extends RaftRpcMessage,
    REPLY extends RaftRpcMessage> {
  public static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

  interface HandlerInterface<REQUEST extends RaftRpcMessage,
      REPLY extends RaftRpcMessage> {

    boolean isAlive();

    REPLY handleRequest(REQUEST r) throws IOException;
  }

  private final Supplier<String> serverIdSupplier;
  private final String name;
  private final SimulatedRequestReply<REQUEST, REPLY> rpc;
  private final HandlerInterface<REQUEST, REPLY> handlerImpl;
  private final List<HandlerDaemon> daemons;

  RequestHandler(Supplier<String> serverIdSupplier, String name,
                 SimulatedRequestReply<REQUEST, REPLY> rpc,
                 HandlerInterface<REQUEST, REPLY> handlerImpl,
                 int numHandlers) {
    this.serverIdSupplier = serverIdSupplier;
    this.name = name;
    this.rpc = rpc;
    this.handlerImpl = handlerImpl;

    this.daemons = new ArrayList<>(numHandlers);
    for(int i = 0; i < numHandlers; i++) {
      daemons.add(new HandlerDaemon(this, i));
    }
  }

  private String getServerId() {
    return serverIdSupplier.get();
  }

  void startDaemon() {
    System.err.println("wangjie startDaemon serverHandler:" + this.hashCode());
    daemons.forEach(Thread::start);
  }

  void shutdown() {
    rpc.shutdown(getServerId());
  }

  void interruptAndJoinDaemon() throws InterruptedException {
    daemons.forEach(Thread::interrupt);
    for (Daemon d : daemons) {
      d.join(1000);
    }
  }

  SimulatedRequestReply<REQUEST, REPLY> getRpc() {
    return rpc;
  }

  void handleRequest(REQUEST request) throws IOException {
    final REPLY reply;
    try {
      System.err.println("wangjie handleRequest 1 handler:" + this.hashCode() + " request:" + request.hashCode());
      reply = handlerImpl.handleRequest(request);
      System.err.println("wangjie handleRequest 2 handler:" + this.hashCode() + " request:" + request.hashCode());
    } catch (IOException ioe) {
      LOG.debug("IOException for " + request, ioe);
      rpc.sendReply(request, null, ioe);
      return;
    }
    if (reply != null) {
      System.err.println("wangjie handleRequest 3 handler:" + this.hashCode() + " request:" + request.hashCode());
      rpc.sendReply(request, reply, null);
      System.err.println("wangjie handleRequest 4 handler:" + this.hashCode() + " request:" + request.hashCode());
    }
  }

  /**
   * A thread keep polling requests from the request queue. Used for simulation.
   */
  class HandlerDaemon extends Daemon {
    private final int id;
    private final RequestHandler handler;

    HandlerDaemon(RequestHandler handler, int id) {
      this.handler = handler;
      this.id = id;
    }

    @Override
    public String toString() {
      return getServerId() + "." + name + id;
    }

    @Override
    public void run() {
      while (handlerImpl.isAlive()) {
        try {
          if (Thread.interrupted()) {
            throw new InterruptedException(this + " was interrupted previously.");
          }
          System.err.println("wangjie thread run handler:" + handler.hashCode() + " serverId:" + getServerId()
            + " queues:" + rpc.getQueues().hashCode() + " size:" + rpc.getQueues().get(getServerId()).getRequestQueue().size());
          handleRequest(rpc.takeRequest(getServerId(), handler));
        } catch (InterruptedIOException e) {
          LOG.info(this + " is interrupted by " + e);
          LOG.trace("TRACE", e);
          break;
        } catch (IOException e) {
          LOG.error(this + " has " + e);
          LOG.trace("TRACE", e);
        } catch(Throwable t) {
          if (!handlerImpl.isAlive()) {
            LOG.info(this + " is stopped.");
            break;
          }
          System.err.println("wangjie exit handler:" + handler.hashCode());
          ExitUtils.terminate(1, this + " is terminating.", t, LOG);
        }
      }
      System.err.println("wangjie exit while handler:" + handler.hashCode() + " isAlive:" + handlerImpl.isAlive());
    }
  }
}
