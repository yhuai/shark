/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package shark.parse

import java.io.Serializable
import java.util.HashSet
import java.util.{List => JavaList}
import java.util.{Set => JavaSet}

import org.apache.hadoop.hive.ql.parse.{ParseContext, GlobalLimitCtx, TaskCompiler}
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.exec.Task
import org.apache.hadoop.hive.ql.hooks.{ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.plan.MoveWork

/*
 * The task compiler for Spark.
 */
class SharkCompiler extends TaskCompiler {

  /*
   * Called to transform tasks into local tasks where possible/desirable
   */
  protected def decideExecMode(rootTasks: JavaList[Task[_ <: Serializable]],
                               ctx: Context, globalLimitCtx: GlobalLimitCtx) {

  }

  /*
   * Called to setup counters for the generated tasks
   */
  protected def generateCountersTask(rootTask: Task[_ <: Serializable]) {

  }

  /*
   * Called at the beginning of the compile phase to have another chance to optimize the operator plan
   */
  override protected def optimizeOperatorPlan(pCtxSet: ParseContext, inputs: JavaSet[ReadEntity],
                                              outputs: JavaSet[WriteEntity]) {
  }

  /*
   * Called after the tasks have been generated to run another round of optimization
   */
  protected def optimizeTaskPlan(rootTasks: JavaList[Task[_ <: Serializable]], pCtx: ParseContext, ctx: Context) {

  }

  /*
   * Called to set the appropriate input format for tasks
   */
  protected def setInputFormat(rootTask: Task[_ <: Serializable]) {

  }

  /*
   * Called to generate the taks tree from the parse context/operator tree
   */
  protected def generateTaskTree(rootTasks: JavaList[Task[_ <: Serializable]], pCtx: ParseContext,
                                 mvTask: JavaList[Task[MoveWork]], inputs: JavaSet[ReadEntity],
                                 outputs: JavaSet[WriteEntity]) {

  }

  override def compile(pCtx: ParseContext, rootTasks: JavaList[Task[_ <: Serializable]],
                       inputs: HashSet[ReadEntity], outputs: HashSet[WriteEntity]) {

  }

}
