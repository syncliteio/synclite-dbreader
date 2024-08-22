/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.dbreader;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class JDBCConnector {

    private static final class InstanceHolder {
        private static final JDBCConnector INSTANCE = createJDBCConnector();        
    }
    
    protected int dstIndex;
    private DataSource dataSource = null;
    private static String connInitStmt = ConfLoader.getInstance().getSrcConnectionInitializationStmt();
    private synchronized DataSource getDataSource()
    {
    	if(dataSource == null)
    	{
    		dataSource = new HikariDataSource(getConfig());
    	}
    	return dataSource;
    }

    protected HikariConfig getConfig() {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(ConfLoader.getInstance().getSrcConnectionString());
		String user = ConfLoader.getInstance().getSrcUser();
		String password = ConfLoader.getInstance().getSrcPassword();
		if (user != null) {
			config.setUsername(user);
		}
		if (password != null) {
			config.setPassword(password);
		}		
		config.setMaximumPoolSize(ConfLoader.getInstance().getSrcDBReaderProcessors() + 1);
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", "250");
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		config.setConnectionTimeout(ConfLoader.getInstance().getSrcConnectionTimeoutS() * 1000);
		config.setAutoCommit(false);
		return config;
    }
    
    private static final JDBCConnector createJDBCConnector() {
    	switch(ConfLoader.getInstance().getSrcType()) {
    	case DUCKDB:
    		return new DuckDBConnector();
    	default:
    		return new JDBCConnector();
    	}
    	
	}

	public void createPool() throws SrcExecutionException
    {
    	try 
    	{
    		dataSource = getDataSource();
    	} catch (Exception e) {
    		throw new SrcExecutionException("Failed to create a connection pool : " + e.getMessage(), e);
    	}    	
    }
    
    protected JDBCConnector() {
    }

    public static JDBCConnector getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public Connection connect() throws SrcExecutionException {
        try {
            //return DriverManager.getConnection(PropsLoader.getInstance().getDstConnStr());
        	Connection c = getDataSource().getConnection();
        	if (connInitStmt != null) {
        		try (Statement s = c.createStatement()) {
        			s.execute(connInitStmt);
        		} catch (SQLException e) {
        			throw new SQLException("Failed to execute connection intialization statement : " + connInitStmt + " : " + e.getMessage(), e);
        		}
        	}
        	return c;
        } catch (SQLException e) {
            throw new SrcExecutionException("Failed to connect to destination database : " + e.getMessage(), e);
        }
    }
}
