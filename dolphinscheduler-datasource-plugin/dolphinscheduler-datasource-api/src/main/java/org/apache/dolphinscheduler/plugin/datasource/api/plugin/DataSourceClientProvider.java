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

package org.apache.dolphinscheduler.plugin.datasource.api.plugin;

import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.DataSourceChannel;
import org.apache.dolphinscheduler.spi.datasource.DataSourceClient;
import org.apache.dolphinscheduler.spi.enums.DbType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;

public class DataSourceClientProvider {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceClientProvider.class);

    private static final long duration = PropertyUtils.getLong(TaskConstants.KERBEROS_EXPIRE_TIME, 24);
    private static final Cache<String, DataSourceClient> uniqueId2dataSourceClientCache = CacheBuilder.newBuilder()
        .expireAfterWrite(duration, TimeUnit.HOURS)
        .removalListener((RemovalListener<String, DataSourceClient>) notification -> {
            try (DataSourceClient closedClient = notification.getValue()) {
                logger.info("Datasource: {} is removed from cache due to expire", notification.getKey());
            }
        })
        .maximumSize(100)
        .build();

    private static final Cache<String, Connection> uniqueId2connectionClientCache = CacheBuilder.newBuilder()
            .expireAfterWrite(6, TimeUnit.HOURS)
            .removalListener((RemovalListener<String, Connection>) notification -> {
                try (Connection closedClient = notification.getValue()) {
                    logger.info("Connection: {} is removed from cache due to expire", notification.getKey());
                }catch (SQLException e){}
            })
            .maximumSize(100)
            .build();


    private DataSourcePluginManager dataSourcePluginManager;

    private DataSourceClientProvider() {
        initDataSourcePlugin();
    }

    private static class DataSourceClientProviderHolder {
        private static final DataSourceClientProvider INSTANCE = new DataSourceClientProvider();
    }

    public static DataSourceClientProvider getInstance() {
        return DataSourceClientProviderHolder.INSTANCE;
    }

    public Connection getConnection(DbType dbType, ConnectionParam connectionParam) throws ExecutionException {
        BaseConnectionParam baseConnectionParam = (BaseConnectionParam) connectionParam;
        String datasourceUniqueId = DataSourceUtils.getDatasourceUniqueId(baseConnectionParam, dbType);
        logger.info("Get connection from datasource {}", datasourceUniqueId);
//
//        ConcurrentMap<String, DataSourceClient> dataSourceClientConcurrentMap = uniqueId2dataSourceClientCache.asMap();
//        Set<String> keySet = dataSourceClientConcurrentMap.keySet();
//        for (String key : keySet){
//            DataSourceClient dataSourceClient = dataSourceClientConcurrentMap.get(key);
//            logger.info("DataSourceClientCache dataSourceClient:{}", dataSourceClient);
//            if (dataSourceClient==null){
//                logger.info("DataSourceClientCache key:{},value:{}", key,"no cache");
//            }else{
//                logger.info("DataSourceClientCache key:{},value:{}", key,dataSourceClient.getConnection());
//            }
//        }
        DataSourceClient dataSourceClient = uniqueId2dataSourceClientCache.get(datasourceUniqueId, () -> {
            Map<String, DataSourceChannel> dataSourceChannelMap = dataSourcePluginManager.getDataSourceChannelMap();
            DataSourceChannel dataSourceChannel = dataSourceChannelMap.get(dbType.getDescp());
            if (null == dataSourceChannel) {
                throw new RuntimeException(String.format("datasource plugin '%s' is not found", dbType.getDescp()));
            }
            DataSourceClient createDataSourceClient = dataSourceChannel.createDataSourceClient(baseConnectionParam, dbType);
            logger.info("DataSourceClientCache create new key:{},value:{},conn:{}", datasourceUniqueId,createDataSourceClient,createDataSourceClient.getConnection());
            return createDataSourceClient;
        });

        Connection connection = dataSourceClient.getConnection();
        logger.info("return druid connection:"+connection);
        return connection;
    }
/*
    public Connection getConnectionByHive(DbType dbType, ConnectionParam connectionParam) throws ExecutionException {
        BaseConnectionParam baseConnectionParam = (BaseConnectionParam) connectionParam;
        String connectionUniqueId = DataSourceUtils.getDatasourceUniqueId(baseConnectionParam, dbType);
        logger.info("Get connection {}", connectionUniqueId);
        Connection connection = uniqueId2connectionClientCache.get(connectionUniqueId, () -> {
            Map<String, DataSourceChannel> dataSourceChannelMap = dataSourcePluginManager.getDataSourceChannelMap();
            DataSourceChannel dataSourceChannel = dataSourceChannelMap.get(dbType.getDescp());
            if (null == dataSourceChannel) {
                throw new RuntimeException(String.format("datasource plugin '%s' is not found", dbType.getDescp()));
            }
            DataSourceClient createDataSourceClient = dataSourceChannel.createDataSourceClient(baseConnectionParam, dbType);
            return createDataSourceClient.getConnection();
        });
        return connection;
    }
*/

    private void initDataSourcePlugin() {
        dataSourcePluginManager = new DataSourcePluginManager();
        dataSourcePluginManager.installPlugin();
    }
}
