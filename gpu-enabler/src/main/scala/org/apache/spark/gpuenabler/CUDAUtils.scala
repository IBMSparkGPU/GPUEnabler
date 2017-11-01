/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.gpuenabler

import org.apache.spark.util.Utils
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.RpcUtils
import org.apache.spark.rpc.RpcEndpoint
import org.apache.spark.{SparkEnv, SparkContext}

object CUDAUtils {
  val sparkUtils = Utils

  def cleanFn[F <: AnyRef](sc: SparkContext, f : F ): F = {
    sc.clean(f)
  }

  val _RpcUtils = RpcUtils

  def _rpcEnv = SparkEnv.get.rpcEnv

  type _RpcEnv = RpcEnv

  type _RpcEndpointRef = RpcEndpointRef

  type _RpcEndpoint = RpcEndpoint

  type _ThreadSafeRpcEndpoint = ThreadSafeRpcEndpoint

  type _RpcCallContext = RpcCallContext
}
