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

package org.apache.dolphinscheduler.plugin.task.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.commons.beanutils.RowSetDynaClass;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourceClientProvider;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.CommonUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.SQLTaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.DataType;
import org.apache.dolphinscheduler.plugin.task.api.enums.Direct;
import org.apache.dolphinscheduler.plugin.task.api.enums.SqlType;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskAlertInfo;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.SqlParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.UdfFuncParameters;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParamUtils;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParameterUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import org.slf4j.Logger;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqlTask extends AbstractTask {

    /**
     * taskExecutionContext
     */
    private TaskExecutionContext taskExecutionContext;

    /**
     * sql parameters
     */
    private SqlParameters sqlParameters;

    /**
     * base datasource
     */
    private BaseConnectionParam baseConnectionParam;

    /**
     * create function format
     * include replace here which can be compatible with more cases, for example a long-running Spark session in Kyuubi will keep its own temp functions instead of destroying them right away
     */
    private static final String CREATE_OR_REPLACE_FUNCTION_FORMAT = "create or replace temporary function {0} as ''{1}''";

    /**
     * default query sql limit
     */
    private static final int QUERY_LIMIT = 10000;

    /**
     * default batch size
     */
    private static final int BATCH_SIZE = 1000;

    private SQLTaskExecutionContext sqlTaskExecutionContext;

    /**
     * Abstract Yarn Task
     *
     * @param taskRequest taskRequest
     */
    public SqlTask(TaskExecutionContext taskRequest) {
        super(taskRequest);
        this.taskExecutionContext = taskRequest;
        this.sqlParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), SqlParameters.class);

        assert sqlParameters != null;
        if (!sqlParameters.checkParameters()) {
            throw new RuntimeException("sql task params is not valid");
        }

        sqlTaskExecutionContext = sqlParameters.generateExtendedContext(taskExecutionContext.getResourceParametersHelper());
    }

    @Override
    public AbstractParameters getParameters() {
        return sqlParameters;
    }

    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        logger.info("sql type : {}, datasource : {}, sql : {} , localParams : {},udfs : {},showType : {},connParams : {},varPool : {} ,query max result limit  {}",
                sqlParameters.getType(),
                sqlParameters.getDatasource(),
                sqlParameters.getSql(),
                sqlParameters.getUdfs(),
                sqlParameters.getShowType(),
                sqlParameters.getConnParams(),
                sqlParameters.getLimit());
        try {

            DbType dbType = DbType.valueOf(sqlParameters.getType());
            // get datasource
            baseConnectionParam = (BaseConnectionParam) DataSourceUtils.buildConnectionParams(
                    dbType, sqlTaskExecutionContext.getConnectionParams());

            Map<String, Property> paramsMap = taskExecutionContext.getPrepareParamsMap();
            Map<String, Property> localParametersMap = sqlParameters.getLocalParametersMap();

            // check is input object
            boolean isObjectListIn = false;
            String objectListValue = null;
            int batchSize = BATCH_SIZE;
            if (localParametersMap!=null){
                Set<String> keySet = localParametersMap.keySet();
                for (String localKey : keySet){
                    Property property = localParametersMap.get(localKey);
                    if (property.getDirect() == Direct.IN
                            && sqlParameters.getSqlType() == SqlType.NON_QUERY.ordinal()){
                        if (property.getProp().equalsIgnoreCase("batchSize")) {
                            batchSize = Integer.parseInt(property.getValue());
                        }
                        if (property.getType() == DataType.OBJECT && paramsMap.containsKey(localKey)){
                            objectListValue = paramsMap.get(localKey).getValue();
                            isObjectListIn = true;
                        }
                    }
                }
            }
            boolean finalIsObjectListIn = isObjectListIn;

            // ready to execute SQL and parameter entity Map
            List<SqlBinds> mainStatementSqlBinds = SqlSplitter.split(sqlParameters.getSql(), sqlParameters.getSegmentSeparator())
                    .stream()
                    .map( x -> getSqlAndSqlParamsMap(x, finalIsObjectListIn))
                    .collect(Collectors.toList());

            //pre sql
            List<String> preSql = Optional.ofNullable(sqlParameters.getPreStatements())
                    .orElse(new ArrayList<>());
            addPreSql(dbType , preSql);

            //ready to execute Pre SQL and parameter entity Map
            List<SqlBinds> preStatementSqlBinds = preSql
                    .stream()
                    .map( x -> getSqlAndSqlParamsMap(x, finalIsObjectListIn))
                    .collect(Collectors.toList());
            //ready to execute Post SQL and parameter entity Map
            List<SqlBinds> postStatementSqlBinds = Optional.ofNullable(sqlParameters.getPostStatements())
                    .orElse(new ArrayList<>())
                    .stream()
                    .map( x -> getSqlAndSqlParamsMap(x, finalIsObjectListIn))
                    .collect(Collectors.toList());
            //create udf
            List<String> createFuncs = createFuncs(sqlTaskExecutionContext.getUdfFuncParametersList(), logger);

            // execute sql task
            executeFuncAndSql(mainStatementSqlBinds, preStatementSqlBinds, postStatementSqlBinds, createFuncs, finalIsObjectListIn,objectListValue, batchSize);

            setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);

        } catch (Exception e) {
            setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
            logger.error("sql task error", e);
            throw new TaskException("Execute sql task failed", e);
        }
    }

    private void addPreSql(DbType dbType,List<String> preStatements ){
        if (dbType.isHive()){
            if (preStatements.isEmpty()){
                preStatements.add("set mapreduce.job.queuename=etl");
                preStatements.add("set compression_codec=snappy");
            }else{
                boolean hasQueue = false;
                for (String preSql : preStatements) {
                    if(preSql.contains("mapreduce.job.queuename")){
                        hasQueue = true;
                    }
                }
                if (!hasQueue){
                    preStatements.add("set mapreduce.job.queuename=etl");
                }
            }
        }
    }

    @Override
    public void cancel() throws TaskException {

    }

    /**
     * execute function and sql
     *
     * @param mainStatementsBinds main statements binds
     * @param preStatementsBinds pre statements binds
     * @param postStatementsBinds post statements binds
     * @param createFuncs create functions
     */
    public void executeFuncAndSql(List<SqlBinds> mainStatementsBinds,
                                  List<SqlBinds> preStatementsBinds,
                                  List<SqlBinds> postStatementsBinds,
                                  List<String> createFuncs,
                                  boolean finalIsObjectListIn,
                                  String objectListValue,
                                    Integer batchSize) throws Exception {
        Connection connection = null;
        try {
            connection = DataSourceClientProvider.getInstance().getConnection(DbType.valueOf(sqlParameters.getType()), baseConnectionParam);
            // create temp function
            if (CollectionUtils.isNotEmpty(createFuncs)) {
                for (String func : createFuncs){
                    logger.info("createFuncs:"+func);
                }
                createTempFunction(connection, createFuncs);
            }

            // pre execute
            executeUpdate(connection, preStatementsBinds, "pre");

            //result is object type
            boolean outListObj = false;
            Property property = null;
            if (!CollectionUtils.isEmpty(sqlParameters.getLocalParams()) && sqlParameters.getLocalParams().size()==1
                    && (property = sqlParameters.getLocalParams().get(0)).getDirect() == Direct.OUT
                    && property.getType() == DataType.OBJECT){
                outListObj = true;
            }

            // main execute
            String result = null;
            ArrayNode arrayNodeResult = null;
            // decide whether to executeQuery or executeUpdate based on sqlType
            if (sqlParameters.getSqlType() == SqlType.QUERY.ordinal()) {
                // query statements need to be convert to JsonArray and inserted into Alert to send
                if (outListObj) {
                    arrayNodeResult = executeQuery(connection, mainStatementsBinds.get(0));
                }else{
                    result = executeQuery(connection, mainStatementsBinds.get(0), "main");
                }
            } else if (sqlParameters.getSqlType() == SqlType.NON_QUERY.ordinal()) {
                // non query statement
                if (finalIsObjectListIn) {
                    executeObjectListUpdate(connection, mainStatementsBinds, objectListValue,batchSize);
                }else{
                    String updateResult = executeUpdate(connection, mainStatementsBinds, "main");
                    result = setNonQuerySqlReturn(updateResult, sqlParameters.getLocalParams());
                }
            }
            //deal out params
            if (outListObj) {
                sqlParameters.dealOutObjectParam(arrayNodeResult,property);
            }else{
                sqlParameters.dealOutParam(result);
            }
            // post execute
            executeUpdate(connection, postStatementsBinds, "post");
        } catch (Exception e) {
            logger.error("execute sql error: {}", e.getMessage());
            throw e;
        } finally {
            logger.info("close connection");
            close(connection);
        }
    }

    private String setNonQuerySqlReturn(String updateResult, List<Property> properties) {
        String result = null;
        for (Property info : properties) {
            if (Direct.OUT == info.getDirect()) {
                List<Map<String, String>> updateRL = new ArrayList<>();
                Map<String, String> updateRM = new HashMap<>();
                updateRM.put(info.getProp(), updateResult);
                updateRL.add(updateRM);
                result = JSONUtils.toJsonString(updateRL);
                break;
            }
        }
        return result;
    }

    /**
     * result process
     *
     * @param resultSet resultSet
     * @throws Exception Exception
     */
    private String resultProcess(ResultSet resultSet) throws Exception {
        ArrayNode resultJSONArray = JSONUtils.createArrayNode();
        if (resultSet != null) {
            ResultSetMetaData md = resultSet.getMetaData();
            int num = md.getColumnCount();

            int rowCount = 0;
            int limit = sqlParameters.getLimit() == 0 ? QUERY_LIMIT : sqlParameters.getLimit();

            while (resultSet.next()) {
                if (rowCount == limit) {
                    logger.info("sql result limit : {} exceeding results are filtered", limit);
                    break;
                }
                ObjectNode mapOfColValues = JSONUtils.createObjectNode();
                for (int i = 1; i <= num; i++) {
                    mapOfColValues.set(md.getColumnLabel(i), JSONUtils.toJsonNode(resultSet.getObject(i)));
                }
                resultJSONArray.add(mapOfColValues);
                rowCount++;
            }
            int displayRows = sqlParameters.getDisplayRows() > 0 ? sqlParameters.getDisplayRows() : TaskConstants.DEFAULT_DISPLAY_ROWS;
            displayRows = Math.min(displayRows, rowCount);
            logger.info("display sql result {} rows as follows:", displayRows);
            for (int i = 0; i < displayRows; i++) {
                String row = JSONUtils.toJsonString(resultJSONArray.get(i));
                logger.info("row {} : {}", i + 1, row);
            }
        }
        String result = JSONUtils.toJsonString(resultJSONArray);
        if (Boolean.TRUE.equals(sqlParameters.getSendEmail())) {
            sendAttachment(sqlParameters.getGroupId(), StringUtils.isNotEmpty(sqlParameters.getTitle())
                    ? sqlParameters.getTitle()
                    : taskExecutionContext.getTaskName() + " query result sets", result);
        }
        logger.debug("execute sql result : {}", result);
        return result;
    }


    /**
     * result process
     *
     * @param resultSet resultSet
     * @throws Exception Exception
     */
    private ArrayNode resultObjectListProcess(ResultSet resultSet) throws Exception {
        ArrayNode resultJSONArray = JSONUtils.createArrayNode();
        if (resultSet != null) {
            ResultSetMetaData md = resultSet.getMetaData();
            int num = md.getColumnCount();

            while (resultSet.next()) {
                ObjectNode mapOfColValues = JSONUtils.createObjectNode();
                for (int i = 1; i <= num; i++) {
                    mapOfColValues.set(md.getColumnLabel(i), JSONUtils.toJsonNode(resultSet.getObject(i)));
                }
                resultJSONArray.add(mapOfColValues);
            }
            int displayRows = sqlParameters.getDisplayRows() > 0 ? sqlParameters.getDisplayRows() : TaskConstants.DEFAULT_DISPLAY_ROWS;
            displayRows = Math.min(displayRows, TaskConstants.DEFAULT_DISPLAY_ROWS);
            logger.info("display sql result {} rows as follows:", displayRows);
            for (int i = 0; i < displayRows; i++) {
                String row = JSONUtils.toJsonString(resultJSONArray.get(i));
                logger.info("row {} : {}", i + 1, row);
            }
        }
        if (Boolean.TRUE.equals(sqlParameters.getSendEmail())) {
            sendAttachment(sqlParameters.getGroupId(), StringUtils.isNotEmpty(sqlParameters.getTitle())
                    ? sqlParameters.getTitle()
                    : taskExecutionContext.getTaskName() + " query result sets", JSONUtils.toJsonString(resultJSONArray));
        }
        return resultJSONArray;
    }

    /**
     * send alert as an attachment
     *
     * @param title title
     * @param content content
     */
    private void sendAttachment(int groupId, String title, String content) {
        setNeedAlert(Boolean.TRUE);
        TaskAlertInfo taskAlertInfo = new TaskAlertInfo();
        taskAlertInfo.setAlertGroupId(groupId);
        taskAlertInfo.setContent(content);
        taskAlertInfo.setTitle(title);
        setTaskAlertInfo(taskAlertInfo);
    }

    private String executeQuery(Connection connection, SqlBinds sqlBinds, String handlerType) throws Exception {
        try (PreparedStatement statement = prepareStatementAndBind(connection, sqlBinds)) {
            ResultSet resultSet = statement.executeQuery();
            return resultProcess(resultSet);
        }
    }

    private ArrayNode executeQuery(Connection connection, SqlBinds sqlBinds) throws Exception {
        try (PreparedStatement statement = prepareStatementAndBind(connection, sqlBinds)) {
            ResultSet resultSet = statement.executeQuery();
            return resultObjectListProcess(resultSet);
        }
    }

    private String executeUpdate(Connection connection, List<SqlBinds> statementsBinds, String handlerType) throws Exception {
        int result = 0;
        for (SqlBinds sqlBind : statementsBinds) {
            logger.info("--multi sql debug :"+ sqlBind.getSql() + "\t param : "+sqlBind.getParamsMap());
            try (PreparedStatement statement = prepareStatementAndBind(connection, sqlBind)) {
                result = statement.executeUpdate();
                logger.info("{} statement execute update result: {}, for sql: {}", handlerType, result, sqlBind.getSql());
            }
        }
        return String.valueOf(result);
    }

    private String executeObjectListUpdate(Connection connection, List<SqlBinds> statementsBinds, String objectListValue,Integer batchSize) throws Exception {
        if (StringUtils.isEmpty(objectListValue)){
            return String.valueOf(0);
        }
        int result = 0;
        for (SqlBinds sqlBind : statementsBinds) {
            String sql = sqlBind.getSql();
            List<Map<String, Object>> objectList = getListMapByString(objectListValue);
            Map<String, Object> firstRow = objectList.get(0);
            Set<String> keySet = firstRow.keySet();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            int n = 0;
            for (Map<String, Object> map : objectList){
                int idx = 1;
                for (String key : keySet){
                    preparedStatement.setObject(idx++ , map.get(key));
                }
                preparedStatement.addBatch();
                if ( (++n % batchSize) == 0){
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                    connection.commit();
                    logger.info("submit size:{} " , n );
                }
            }
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
            connection.commit();
            logger.info("update size:{} " , n );
        }

        return String.valueOf(result);
    }


    public List<Map<String, Object>> getListMapByString(String json) {
        List<Map<String, Object>> allParams = new ArrayList<>();
        ArrayNode paramsByJson = JSONUtils.parseArray(json);
        for (JsonNode jsonNode : paramsByJson) {
            Map<String, Object> param = JSONUtils.toObjectMap(jsonNode.toString());
            allParams.add(param);
        }
        return allParams;
    }

    /**
     * create temp function
     *
     * @param connection connection
     * @param createFuncs createFuncs
     */
    private void createTempFunction(Connection connection,
                                    List<String> createFuncs) throws Exception {
        try (Statement funcStmt = connection.createStatement()) {
            for (String createFunc : createFuncs) {
                logger.info("hive create function sql: {}", createFunc);
                funcStmt.execute(createFunc);
            }
        }
    }

    /**
     * close jdbc resource
     *
     * @param connection connection
     */
    private void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("close connection error : {}", e.getMessage(), e);
            }
        }
    }

    /**
     * preparedStatement bind
     *
     * @param connection connection
     * @param sqlBinds sqlBinds
     * @return PreparedStatement
     * @throws Exception Exception
     */
    private PreparedStatement prepareStatementAndBind(Connection connection, SqlBinds sqlBinds) {
        // is the timeout set
        boolean timeoutFlag = taskExecutionContext.getTaskTimeoutStrategy() == TaskTimeoutStrategy.FAILED
                || taskExecutionContext.getTaskTimeoutStrategy() == TaskTimeoutStrategy.WARNFAILED;
        try {
            PreparedStatement stmt = connection.prepareStatement(sqlBinds.getSql());
            if (timeoutFlag) {
                stmt.setQueryTimeout(taskExecutionContext.getTaskTimeout());
            }
            Map<Integer, Property> params = sqlBinds.getParamsMap();
            if (params != null) {
                for (Map.Entry<Integer, Property> entry : params.entrySet()) {
                    Property prop = entry.getValue();
                    ParameterUtils.setInParameter(entry.getKey(), stmt, prop.getType(), prop.getValue());
                }
            }
            logger.info("prepare statement replace sql : {}, sql parameters : {}", sqlBinds.getSql(), sqlBinds.getParamsMap());
            return stmt;
        } catch (Exception exception) {
            throw new TaskException("SQL task prepareStatementAndBind error", exception);
        }
    }

    /**
     * print replace sql
     *
     * @param content content
     * @param formatSql format sql
     * @param rgex rgex
     * @param sqlParamsMap sql params map
     */
    private void printReplacedSql(String content, String formatSql, String rgex, Map<Integer, Property> sqlParamsMap) {
        //parameter print style
        logger.info("after replace sql , preparing : {}", formatSql);
        StringBuilder logPrint = new StringBuilder("replaced sql , parameters:");
        if (sqlParamsMap == null) {
            logger.info("printReplacedSql: sqlParamsMap is null.");
        } else {
            for (int i = 1; i <= sqlParamsMap.size(); i++) {
                logPrint.append(sqlParamsMap.get(i).getValue()).append("(").append(sqlParamsMap.get(i).getType()).append(")");
            }
        }
        logger.info("Sql Params are {}", logPrint);
    }

    /**
     * ready to execute SQL and parameter entity Map
     *
     * @return SqlBinds
     */
    private SqlBinds getSqlAndSqlParamsMap(String sql,boolean isObjectListIn) {
        Map<Integer, Property> sqlParamsMap = new HashMap<>();
        StringBuilder sqlBuilder = new StringBuilder();
        // new
        // replace variable TIME with $[YYYYmmddd...] in sql when history run job and batch complement job
        sql = ParameterUtils.replaceScheduleTime(sql, taskExecutionContext.getScheduleTime());
        // combining local and global parameters
        Map<String, Property> paramsMap = taskExecutionContext.getPrepareParamsMap();

        if (isObjectListIn){
            sqlBuilder.append(sql);
            return new SqlBinds(sqlBuilder.toString(), sqlParamsMap);
        }else{
            // spell SQL according to the final user-defined variable
            if (paramsMap == null) {
                sqlBuilder.append(sql);
                return new SqlBinds(sqlBuilder.toString(), sqlParamsMap);
            }

            if (StringUtils.isNotEmpty(sqlParameters.getTitle())) {
                String title = ParameterUtils.convertParameterPlaceholders(sqlParameters.getTitle(),
                        ParamUtils.convert(paramsMap));
                logger.info("SQL title : {}", title);
                sqlParameters.setTitle(title);
            }

            // special characters need to be escaped, ${} needs to be escaped
            setSqlParamsMap(sql, rgex, sqlParamsMap, paramsMap,taskExecutionContext.getTaskInstanceId());
            //Replace the original value in sql ！{...} ，Does not participate in precompilation
            String rgexo = "['\"]*\\!\\{(.*?)\\}['\"]*";
            sql = replaceOriginalValue(sql, rgexo, paramsMap);
            // replace the ${} of the SQL statement with the Placeholder
            String formatSql = sql.replaceAll(rgex, "?");
            // Convert the list parameter
            formatSql = ParameterUtils.expandListParameter(sqlParamsMap, formatSql);
            sqlBuilder.append(formatSql);
            // print replace sql
            printReplacedSql(sql, formatSql, rgex, sqlParamsMap);
            return new SqlBinds(sqlBuilder.toString(), sqlParamsMap);
        }
    }

    private String replaceOriginalValue(String content, String rgex, Map<String, Property> sqlParamsMap) {
        Pattern pattern = Pattern.compile(rgex);
        while (true) {
            Matcher m = pattern.matcher(content);
            if (!m.find()) {
                break;
            }
            String paramName = m.group(1);
            String paramValue = sqlParamsMap.get(paramName).getValue();
            content = m.replaceFirst(paramValue);
        }
        return content;
    }

    /**
     * create function list
     *
     * @param udfFuncParameters udfFuncParameters
     * @param logger logger
     * @return
     */
    private List<String> createFuncs(List<UdfFuncParameters> udfFuncParameters, Logger logger) {

        if (CollectionUtils.isEmpty(udfFuncParameters)) {
            logger.info("can't find udf function resource");
            return null;
        }
        // build jar sql
        List<String> funcList = buildJarSql(udfFuncParameters);

        // build temp function sql
        List<String> tempFuncList = buildTempFuncSql(udfFuncParameters);
        funcList.addAll(tempFuncList);
        return funcList;
    }

    /**
     * build temp function sql
     * @param udfFuncParameters udfFuncParameters
     * @return
     */
    private List<String> buildTempFuncSql(List<UdfFuncParameters> udfFuncParameters) {
        return udfFuncParameters.stream().map(value -> MessageFormat
                .format(CREATE_OR_REPLACE_FUNCTION_FORMAT, value.getFuncName(), value.getClassName())).collect(Collectors.toList());
    }

    /**
     * build jar sql
     * @param udfFuncParameters udfFuncParameters
     * @return
     */
    private List<String> buildJarSql(List<UdfFuncParameters> udfFuncParameters) {
        return udfFuncParameters.stream().map(value -> {
            String defaultFS = value.getDefaultFS();
            String prefixPath = defaultFS.startsWith("file://") ? "file://" : defaultFS;
            String uploadPath = CommonUtils.getHdfsUdfDir(value.getTenantCode());
            String resourceFullName = value.getResourceName();
            resourceFullName = resourceFullName.startsWith("/") ? resourceFullName : String.format("/%s", resourceFullName);
            return String.format("add jar %s%s%s", prefixPath, uploadPath, resourceFullName);
        }).collect(Collectors.toList());
    }

}
