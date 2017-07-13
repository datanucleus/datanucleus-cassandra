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

import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.CassandraSchemaHandler;
import org.datanucleus.store.cassandra.CassandraStoreManager;
import org.datanucleus.store.cassandra.SessionStatementProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.valuegenerator.AbstractConnectedGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Value generator using a table in the datastore and incrementing a column, keyed by the field name that has
 * the strategy.
 */
public class IncrementGenerator extends AbstractConnectedGenerator<Long>
{
    private String key = null;

    private String schemaName = null;

    private String tableName = "incrementtable";

    private String keyColName = "key";

    private String valColName = "value";

    /** Flag for whether we know that the repository exists. */
    protected boolean repositoryExists = false;

    public IncrementGenerator(StoreManager storeMgr, String name, Properties props)
    {
        super(storeMgr, name, props);

        if (properties.getProperty(ValueGenerator.PROPERTY_SEQUENCE_NAME) != null)
        {
            // Specified sequence-name so use that
            key = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCE_NAME);
        }
        else if (properties.containsKey(ValueGenerator.PROPERTY_FIELD_NAME))
        {
            // Use field name
            key = properties.getProperty(ValueGenerator.PROPERTY_FIELD_NAME);
        }
        else
        {
            // Use root class name (for this inheritance tree) in the sequence table as the sequence name
            key = properties.getProperty(ValueGenerator.PROPERTY_ROOT_CLASS_NAME);
        }

        if (properties.containsKey(ValueGenerator.PROPERTY_SEQUENCETABLE_TABLE))
        {
            tableName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_TABLE);
        }
        if (properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_NAME_COLUMN) != null)
        {
            keyColName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_NAME_COLUMN);
        }
        if (properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_NEXTVAL_COLUMN) != null)
        {
            valColName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_NEXTVAL_COLUMN);
        }

        if (properties.containsKey(ValueGenerator.PROPERTY_KEY_CACHE_SIZE))
        {
            allocationSize = Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_CACHE_SIZE));
        }
        else
        {
            allocationSize = 1;
        }
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.valuegenerator.AbstractGenerator#reserveBlock(long)
     */
    @Override
    protected ValueGenerationBlock<Long> reserveBlock(long size)
    {
        if (size < 1)
        {
            return null;
        }

        if (!repositoryExists)
        {
            createRepository();
        }

        List<Long> oids = new ArrayList<Long>();
        ManagedConnection mconn = connectionProvider.retrieveConnection();
        try
        {
            Session session = (Session) mconn.getConnection();

            StringBuilder stmtBuilder = new StringBuilder("SELECT ");
            stmtBuilder.append(valColName).append(" FROM ").append(getSchemaName()).append('.').append(tableName).append(" WHERE ").append(keyColName).append("=?");
            NucleusLogger.VALUEGENERATION.debug("Getting current value for increment strategy for key=" + key + " : " + stmtBuilder.toString());
            SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
            PreparedStatement stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
            ResultSet rs = session.execute(stmt.bind(key));
            if (rs.isExhausted())
            {
                long initialValue = 0;
                if (properties.containsKey(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE))
                {
                    initialValue = Long.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_INITIAL_VALUE));
                }

                // No existing values, so add new row, starting at end of this first block
                stmtBuilder = new StringBuilder("INSERT INTO ");
                stmtBuilder.append(getSchemaName()).append('.').append(tableName).append("(").append(keyColName).append(',').append(valColName).append(") VALUES(?,?)");
                NucleusLogger.VALUEGENERATION.debug("Setting value for increment strategy for key=" + key + " val=" + (initialValue + size + 1) + " : " + stmtBuilder.toString());
                stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
                session.execute(stmt.bind(key, (initialValue + size + 1)));

                for (int i = 0; i < size; i++)
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
                NucleusLogger.VALUEGENERATION.debug("Setting next value for increment strategy for key=" + key + " val=" + (val + size) + " : " + stmtBuilder.toString());
                stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
                session.execute(stmt.bind(key, (val + size)));

                for (int i = 0; i < size; i++)
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

        schemaName = properties.getProperty(ValueGenerator.PROPERTY_SCHEMA_NAME);
        if (schemaName == null)
        {
            schemaName = storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_SCHEMA);
        }
        return schemaName;
    }

    protected boolean createRepository()
    {
        if (repositoryExists)
        {
            return true;
        }

        ManagedConnection mconn = connectionProvider.retrieveConnection();
        try
        {
            Session session = (Session) mconn.getConnection();

            if (CassandraSchemaHandler.getTableMetadata(session, getSchemaName(), tableName) != null)
            {
                // Already exists
                repositoryExists = true;
                return true;
            }

            if (storeMgr.getSchemaHandler().isAutoCreateTables())
            {
                StringBuilder stmtBuilder = new StringBuilder("CREATE TABLE ");
                stmtBuilder.append(getSchemaName()).append('.').append(tableName).append("(");
                stmtBuilder.append(keyColName).append(" varchar,").append(valColName).append(" bigint,PRIMARY KEY(").append(keyColName).append(")");
                stmtBuilder.append(")");
                NucleusLogger.VALUEGENERATION.debug("Creating value generator table : " + stmtBuilder.toString());
                session.execute(stmtBuilder.toString());
                repositoryExists = true;
            }
            else
            {
                throw new NucleusUserException("Table for increment strategy doesn't exist, but autoCreateTables is set to false. Set it to true");
            }
        }
        finally
        {
            connectionProvider.releaseConnection();
        }
        return true;
    }
}