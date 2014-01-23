/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.cassandra.valuegenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.cassandra.CassandraStoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Value generator using a table in the datastore and incrementing a column, keyed by the field name that has the strategy.
 */
public class IncrementGenerator extends AbstractDatastoreGenerator
{
    static final String INCREMENT_COL_NAME = "increment";

    private String key = null;

    private String schemaName = null;
    private String tableName = "incrementtable";

    private String keyColName = "key";

    private String valColName = "value";

    public IncrementGenerator(String name, Properties props)
    {
        super(name, props);

        this.key = properties.getProperty("field-name", name);

        if (properties.getProperty("sequence-table-name") != null)
        {
            tableName = "IncrementTable";
        }
        if (properties.getProperty("sequence-name-column-name") != null)
        {
            keyColName = properties.getProperty("sequence-name-column-name");
        }
        if (properties.getProperty("sequence-nextval-column-name") != null)
        {
            valColName = properties.getProperty("sequence-nextval-column-name");
        }

        if (properties.containsKey("key-cache-size"))
        {
            allocationSize = Integer.valueOf(properties.getProperty("key-cache-size"));
        }
        else
        {
            allocationSize = 1;
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.valuegenerator.AbstractGenerator#reserveBlock(long)
     */
    @Override
    protected ValueGenerationBlock reserveBlock(long size)
    {
        if (size < 1)
        {
            return null;
        }

        if (!repositoryExists)
        {
            createRepository();
        }

        List oids = new ArrayList();
        ManagedConnection mconn = connectionProvider.retrieveConnection();
        try
        {
            Session session = (Session) mconn.getConnection();

            StringBuilder stmtBuilder = new StringBuilder("SELECT ");
            stmtBuilder.append(valColName).append(" FROM ").append(getSchemaName()).append('.').append(tableName).append(" WHERE ").append(keyColName).append("=?");
            NucleusLogger.VALUEGENERATION.debug("Getting current value for increment strategy for key=" + key + " : " + stmtBuilder.toString());
            PreparedStatement stmt = session.prepare(stmtBuilder.toString());
            ResultSet rs = session.execute(stmt.bind(key));
            if (rs.isExhausted())
            {
                long initialValue = 0;
                if (properties.containsKey("key-initial-value"))
                {
                    initialValue = Long.valueOf(properties.getProperty("key-initial-value"));
                }

                // No existing values, so add new row, starting at end of this first block
                stmtBuilder = new StringBuilder("INSERT INTO ");
                stmtBuilder.append(getSchemaName()).append('.').append(tableName).append("(").append(keyColName).append(',').append(valColName).append(") VALUES(?,?)");
                NucleusLogger.VALUEGENERATION.debug("Setting value for increment strategy for key=" + key + " val=" + (initialValue+size+1) + " : " + stmtBuilder.toString());
                stmt = session.prepare(stmtBuilder.toString());
                session.execute(stmt.bind(key, (initialValue+size+1)));

                for (int i=0;i<size;i++)
                {
                    oids.add(initialValue + i);
                }
            }
            else
            {
                Row row = rs.one();
                long val = row.getLong(valColName.toLowerCase());

                // Update value allowing for this block
                stmtBuilder = new StringBuilder("INSERT INTO ");
                stmtBuilder.append(getSchemaName()).append('.').append(tableName).append("(").append(keyColName).append(',').append(valColName).append(") VALUES(?,?)");
                NucleusLogger.VALUEGENERATION.debug("Setting next value for increment strategy for key=" + key + " val=" + (val+size) + " : " + stmtBuilder.toString());
                stmt = session.prepare(stmtBuilder.toString());
                session.execute(stmt.bind(key, (val+size)));

                for (int i=0;i<size;i++)
                {
                    oids.add(val + i);
                }
            }
        }
        finally
        {
            connectionProvider.releaseConnection();
        }

        return new ValueGenerationBlock(oids);
    }

    protected String getSchemaName()
    {
        if (schemaName != null)
        {
            return schemaName;
        }
        else
        {
            schemaName = properties.getProperty("schema-name");
            if (schemaName == null)
            {
                schemaName = ((CassandraStoreManager)storeMgr).getSchemaName();
            }
            return schemaName;
        }
    }

    protected boolean createRepository()
    {
        if (repositoryExists)
        {
            return true;
        }

        if (storeMgr.isAutoCreateTables())
        {
            ManagedConnection mconn = connectionProvider.retrieveConnection();
            try
            {
                Session session = (Session) mconn.getConnection();

                StringBuilder stmtBuilder = new StringBuilder("CREATE TABLE ");
                stmtBuilder.append(getSchemaName()).append('.').append(tableName).append("(");
                stmtBuilder.append(keyColName).append(" varchar,").append(valColName).append(" bigint,PRIMARY KEY(").append(keyColName).append(")");
                stmtBuilder.append(")");
                NucleusLogger.VALUEGENERATION.debug("Creating value generator table : " + stmtBuilder.toString());
                session.execute(stmtBuilder.toString());
                repositoryExists = true;
            }
            finally
            {
                connectionProvider.releaseConnection();
            }
        }
        else
        {
            throw new NucleusUserException("Table for increment strategy doesn't exist, but autoCreateTables is set to false. Set it to true");
        }
        return true;
    }
}
