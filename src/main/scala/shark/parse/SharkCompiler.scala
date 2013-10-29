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
import java.util.{Map => JavaMap, List => JavaList, Set => JavaSet, ArrayList, HashSet}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.parse.TaskCompiler
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.exec.{DDLTask, FetchTask, MoveTask, TaskFactory, Task}
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator}
import org.apache.hadoop.hive.ql.hooks.{ReadEntity, WriteEntity}
import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.Warehouse
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.metastore.api.{FieldSchema, MetaException}

import scala.Some

import shark.{LogHelper, CachedTableRecovery, SharkEnv, SharkConfVars}
import shark.memstore2.{CacheType, ColumnarSerDe, MemoryMetadataManager}
import shark.execution.{HiveOperator, Operator, OperatorFactory, RDDUtils, ReduceSinkOperator, SparkWork, TerminalOperator}


/*
 * The task compiler for Spark.
 */
class SharkCompiler extends TaskCompiler with LogHelper {

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

  private def convertRowSchemaToViewSchema(rr: RowResolver): JavaList[FieldSchema] = {
    val fieldSchemas: JavaList[FieldSchema] = new ArrayList[FieldSchema]
    for (colInfo <- rr.getColumnInfos) {
      if (!colInfo.isHiddenVirtualCol) {
        val colName: String = rr.reverseLookup(colInfo.getInternalName)(1)
        fieldSchemas.add(new FieldSchema(colName, colInfo.getType.getTypeName, null))
      }
    }
    fieldSchemas
  }

  override def compile(pCtx: ParseContext, rootTasks: JavaList[Task[_ <: Serializable]],
                       inputs: HashSet[ReadEntity], outputs: HashSet[WriteEntity]) {

    var cacheMode = CacheType.NONE
    var shouldReset = false

    val qb: QB = pCtx.getQB
    val opParseCtx = pCtx.getOpParseCtx
    var _resSchema: JavaList[FieldSchema] = null

    /*
     * Step 1: Analyze create table.
     * For a query for creating a table, if it is not CATS, we will not reach here.
     * Can we use HiveSemanticAnalyzerHook? We can get current DB name from SessionState.get.getCurrentDatabase
     * Seems work in SharkDDLSemanticAnalyzer can be done by a hook?
     */


    // If the table descriptor can be null if the CTAS has an
    // "if not exists" condition.
    val createTableDesc = pCtx.getQB.getTableDesc
    if (!qb.isCTAS || createTableDesc == null) {
      return
    } else {
      val checkTableName = SharkConfVars.getBoolVar(conf, SharkConfVars.CHECK_TABLENAME_FLAG)
      // Note: the CreateTableDesc's table properties are Java Maps, but the TableDesc's table
      //       properties, which are used during execution, are Java Properties.
      val createTableProperties: JavaMap[String, String] = createTableDesc.getTblProps()

      // There are two cases that will enable caching:
      // 1) Table name includes "_cached" or "_tachyon".
      // 2) The "shark.cache" table property is "true", or the string representation of a supported
      //   cache mode (heap, Tachyon).
      cacheMode = CacheType.fromString(createTableProperties.get("shark.cache"))
      // Continue planning based on the 'cacheMode' read.
      if (cacheMode == CacheType.HEAP ||
        (createTableDesc.getTableName.endsWith("_cached") && checkTableName)) {
        cacheMode = CacheType.HEAP
        createTableProperties.put("shark.cache", cacheMode.toString)
      } else if (cacheMode == CacheType.TACHYON ||
        (createTableDesc.getTableName.endsWith("_tachyon") && checkTableName)) {
        cacheMode = CacheType.TACHYON
        createTableProperties.put("shark.cache", cacheMode.toString)
      }

      if (CacheType.shouldCache(cacheMode)) {
        createTableDesc.setSerName(classOf[ColumnarSerDe].getName)
      }

      qb.setTableDesc(createTableDesc)
      shouldReset = true
    }

    // Reset makes sure we don't run the mapred jobs generated by Hive.
    // if (shouldReset) reset() do we have to reset?

    if (pCtx.getFsopToTable.keySet().size() > 1) {
      throw new SemanticException("Shark does not support single query multi insert");
    } else {
      val hiveSinkOp = pCtx.getFsopToTable.keySet().head;
      _resSchema = convertRowSchemaToViewSchema(opParseCtx.get(hiveSinkOp).getRowResolver)
    }


    // Replace Hive physical plan with Shark plan. This needs to happen after
    // Hive optimization.
    val hiveSinkOps = SharkCompiler.findAllHiveFileSinkOperators(
      pCtx.getTopOps().values().head)

    // TODO: clean the following code. It's too messy to understand...
    val terminalOpSeq = {
      if (qb.getParseInfo.isInsertToTable && !qb.isCTAS) {
        hiveSinkOps.map { hiveSinkOp =>
          val tableDesc = hiveSinkOp.asInstanceOf[HiveFileSinkOperator].getConf().getTableInfo();
          val tableName = tableDesc.getTableName()
          val tableDBName = tableDesc.getProperties.getProperty(
            org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_DB);

          if (tableName == null || tableName == "") {
            // If table name is empty, it is an INSERT (OVERWRITE) DIRECTORY.
            OperatorFactory.createSharkFileOutputPlan(hiveSinkOp)
          } else {
            // Otherwise, check if we are inserting into a table that was cached.
            val cachedTableName = tableName.split('.')(1) // Ignore the database name
            SharkEnv.memoryMetadataManager.get(cachedTableName) match {
              case Some(rdd) => {
                if (hiveSinkOps.size == 1) {
                  // If useUnionRDD is false, the sink op is for INSERT OVERWRITE.
                  val useUnionRDD = qb.getParseInfo.isInsertIntoTable(tableDBName, cachedTableName)
                  val storageLevel = RDDUtils.getStorageLevelOfCachedTable(rdd)
                  OperatorFactory.createSharkMemoryStoreOutputPlan(
                    hiveSinkOp,
                    cachedTableName,
                    storageLevel,
                    _resSchema.size,                // numColumns
                    cacheMode == CacheType.TACHYON, // use tachyon
                    useUnionRDD)
                } else {
                  throw new SemanticException(
                    "Shark does not support updating cached table(s) with multiple INSERTs")
                }
              }
              case None => OperatorFactory.createSharkFileOutputPlan(hiveSinkOp)
            }
          }
        }
      } else if (hiveSinkOps.size == 1) {
        // For a single output, we have the option of choosing the output
        // destination (e.g. CTAS with table property "shark.cache" = "true").
        Seq {
          if (qb.isCTAS && qb.getTableDesc != null && CacheType.shouldCache(cacheMode)) {
            val storageLevel = MemoryMetadataManager.getStorageLevelFromString(
              qb.getTableDesc().getTblProps.get("shark.cache.storageLevel"))
            qb.getTableDesc().getTblProps().put(CachedTableRecovery.QUERY_STRING, pCtx.getContext.getCmd())
            OperatorFactory.createSharkMemoryStoreOutputPlan(
              hiveSinkOps.head,
              qb.getTableDesc.getTableName,
              storageLevel,
              _resSchema.size,                // numColumns
              cacheMode == CacheType.TACHYON, // use tachyon
              false)
          } else if (pCtx.getContext().asInstanceOf[QueryContext].useTableRddSink && !qb.isCTAS) {
            OperatorFactory.createSharkRddOutputPlan(hiveSinkOps.head)
          } else {
            OperatorFactory.createSharkFileOutputPlan(hiveSinkOps.head)
          }
        }

        // A hack for the query plan dashboard to get the query plan. This was
        // done for SIGMOD demo. Turn it off by default.
        //shark.dashboard.QueryPlanDashboardHandler.terminalOperator = terminalOp

      } else {
        // For non-INSERT commands, if there are multiple file outputs, we always use file outputs.
        hiveSinkOps.map(OperatorFactory.createSharkFileOutputPlan(_))
      }
    }

    SharkCompiler.breakHivePlanByStages(terminalOpSeq)
    genMapRedTasks(qb, pCtx, terminalOpSeq, rootTasks, _resSchema, inputs, outputs)

    logDebug("Completed plan generation")
  }

  /**
   * Generate tasks for executing the query, including the SparkTask to do the
   * select, the MoveTask for updates, and the DDLTask for CTAS.
   */
  def genMapRedTasks(qb: QB, pctx: ParseContext, terminalOps: Seq[TerminalOperator],
                     rootTasks: JavaList[Task[_ <: Serializable]],
                     resultSchaema: JavaList[FieldSchema],
                     inputs: HashSet[ReadEntity], outputs: HashSet[WriteEntity]) {

    // Create the spark task.
    terminalOps.foreach { terminalOp =>
      val task = TaskFactory.get(new SparkWork(pctx, terminalOp, resultSchaema), conf)
      rootTasks.add(task)
    }

    if (qb.getIsQuery) {
      // Configure FetchTask (used for fetching results to CLIDriver).
      val loadWork = pctx.getLoadFileWork.get(0)
      val cols = loadWork.getColumns
      val colTypes = loadWork.getColumnTypes

      val resFileFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT)
      val resultTab = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, resFileFormat)

      val fetchWork = new FetchWork(
        new Path(loadWork.getSourceDir).toString, resultTab, qb.getParseInfo.getOuterQueryLimit)

      val fetchTask = TaskFactory.get(fetchWork, conf).asInstanceOf[FetchTask]
      pctx.setFetchTask(fetchTask)
    } else {
      // Configure MoveTasks for table updates (e.g. CTAS, INSERT).
      val mvTasks = new ArrayList[MoveTask]()

      val fileWork = pctx.getLoadFileWork
      val tableWork = pctx.getLoadTableWork
      tableWork.foreach { ltd =>
        mvTasks.add(TaskFactory.get(
          new MoveWork(null, null, ltd, null, false), conf).asInstanceOf[MoveTask])
      }

      fileWork.foreach { lfd =>
        if (qb.isCTAS) {
          var location = qb.getTableDesc.getLocation
          if (location == null) {
            try {
              val dumpTable = db.newTable(qb.getTableDesc.getTableName)
              val wh = new Warehouse(conf)
              location = wh.getTablePath(db.getDatabase(dumpTable.getDbName()), dumpTable
                .getTableName()).toString;
            } catch {
              case e: HiveException => throw new SemanticException(e)
              case e: MetaException => throw new SemanticException(e)
            }
          }
          lfd.setTargetDir(location)
        }

        mvTasks.add(TaskFactory.get(
          new MoveWork(null, null, null, lfd, false), conf).asInstanceOf[MoveTask])
      }

      // The move task depends on all root tasks. In the case of multi outputs,
      // the moves are only started once all outputs are executed.
      val hiveFileSinkOp = terminalOps.head.hiveOp
      mvTasks.foreach { moveTask =>
        rootTasks.foreach { rootTask =>
          rootTask.addDependentTask(moveTask)
        }

        // Add StatsTask's. See GenMRFileSink1.addStatsTask().
        /*
        if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
          println("Adding a StatsTask for MoveTask " + moveTask)
          //addStatsTask(fsOp, mvTask, currTask, parseCtx.getConf())
          val statsWork = new StatsWork(moveTask.getWork().getLoadTableWork())
          statsWork.setAggKey(hiveFileSinkOp.getConf().getStatsAggPrefix())
          val statsTask = TaskFactory.get(statsWork, conf)
          hiveFileSinkOp.getConf().setGatherStats(true)
          moveTask.addDependentTask(statsTask)
          statsTask.subscribeFeed(moveTask)
        }
        */
      }
    }

    // For CTAS, generate a DDL task to create the table. This task should be a
    // dependent of the main SparkTask.
    if (qb.isCTAS) {
      val crtTblDesc: CreateTableDesc = qb.getTableDesc

      // Use reflection to call validateCreateTable, which is private.
      val validateCreateTableMethod = this.getClass.getSuperclass.getDeclaredMethod(
        "validateCreateTable", classOf[CreateTableDesc])
      validateCreateTableMethod.setAccessible(true)
      validateCreateTableMethod.invoke(this, crtTblDesc)

      // Clear the output for CTAS since we don't need the output from the
      // mapredWork, the DDLWork at the tail of the chain will have the output.
      outputs.clear()

      // CTAS assumes only single output.
      val crtTblTask = TaskFactory.get(
        new DDLWork(inputs, outputs, crtTblDesc),conf).asInstanceOf[DDLTask]
      rootTasks.head.addDependentTask(crtTblTask)
    }
  }
}

object SharkCompiler extends LogHelper {

  /**
   * The reflection object used to invoke convertRowSchemaToViewSchema.
   */
  private val convertRowSchemaToViewSchemaMethod = classOf[SemanticAnalyzer].getDeclaredMethod(
    "convertRowSchemaToViewSchema", classOf[RowResolver])
  convertRowSchemaToViewSchemaMethod.setAccessible(true)

  /**
   * The reflection object used to get a reference to SemanticAnalyzer.viewsExpanded,
   * so we can initialize it.
   */
  private val viewsExpandedField = classOf[SemanticAnalyzer].getDeclaredField("viewsExpanded")
  viewsExpandedField.setAccessible(true)

  /**
   * Given a Hive top operator (e.g. TableScanOperator), find all the file sink
   * operators (aka file output operator).
   */
  private def findAllHiveFileSinkOperators(op: HiveOperator): Seq[HiveOperator] = {
    if (op.getChildOperators() == null || op.getChildOperators().size() == 0) {
      Seq[HiveOperator](op)
    } else {
      op.getChildOperators().flatMap(findAllHiveFileSinkOperators(_)).distinct
    }
  }

  /**
   * Break the Hive operator tree into multiple stages, separated by Hive
   * ReduceSink. This is necessary because the Hive operators after ReduceSink
   * cannot be initialized using ReduceSink's output object inspector. We
   * craft the struct object inspector (that has both KEY and VALUE) in Shark
   * ReduceSinkOperator.initializeDownStreamHiveOperators().
   */
  private def breakHivePlanByStages(terminalOps: Seq[TerminalOperator]) = {
    val reduceSinks = new scala.collection.mutable.HashSet[ReduceSinkOperator]
    val queue = new scala.collection.mutable.Queue[Operator[_]]
    queue ++= terminalOps

    while (!queue.isEmpty) {
      val current = queue.dequeue()
      current match {
        case op: ReduceSinkOperator => reduceSinks += op
        case _ => Unit
      }
      // This is not optimal because operators can be added twice. But the
      // operator tree should not be too big...
      queue ++= current.parentOperators
    }

    logDebug("Found %d ReduceSinkOperator's.".format(reduceSinks.size))

    reduceSinks.foreach { op =>
      val hiveOp = op.asInstanceOf[Operator[HiveOperator]].hiveOp
      if (hiveOp.getChildOperators() != null) {
        hiveOp.getChildOperators().foreach { child =>
          logDebug("Removing child %s from %s".format(child, hiveOp))
          hiveOp.removeChild(child)
        }
      }
    }
  }
}