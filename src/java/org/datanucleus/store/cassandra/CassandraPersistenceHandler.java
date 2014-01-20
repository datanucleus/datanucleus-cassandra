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
package org.datanucleus.store.cassandra;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.naming.NamingFactory;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.datastax.driver.core.Session;

/**
 * Handler for basic persistence operations with Cassandra datastores.
 */
public class CassandraPersistenceHandler extends AbstractPersistenceHandler
{
    protected static final Localiser LOCALISER_CASSANDRA = Localiser.getInstance(
        "org.datanucleus.store.mongodb.Localisation", CassandraStoreManager.class.getClassLoader());

    public CassandraPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    public void close()
    {
    }

    public void insertObject(ObjectProvider op)
    {
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            Session session = (Session)mconn.getConnection();

            AbstractClassMetaData cmd = op.getClassMetaData();
            if (!storeMgr.managesClass(cmd.getFullClassName()))
            {
                // Make sure schema exists, using this connection
                ((CassandraStoreManager)storeMgr).addClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_CASSANDRA.msg("Cassandra.Insert.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            // Create PreparedStatement and values to bind
            NamingFactory namingFactory = storeMgr.getNamingFactory();
            StringBuilder insertStmt = new StringBuilder("INSERT INTO ");
            String schemaName = ((CassandraStoreManager)storeMgr).getSchemaNameForClass(cmd);
            if (schemaName != null)
            {
                insertStmt.append(schemaName).append('.');
            }
            insertStmt.append(namingFactory.getTableName(cmd)).append("(");
            StringBuilder insertValuesStr = new StringBuilder("(");
            List fieldValues = new ArrayList();
            AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
            for (int i = 0;i<mmds.length;i++)
            {
                AbstractMemberMetaData mmd = mmds[i];
                if (i > 0)
                {
                    insertStmt.append(',');
                    insertValuesStr.append(',');
                }
                insertStmt.append(namingFactory.getColumnName(mmd, ColumnType.COLUMN));
                insertValuesStr.append('?');
                Object val = op.provideField(mmd.getAbsoluteFieldNumber());
                // TODO Convert this value into the type that will be stored - need helper method
                fieldValues.add(val);
            }
            insertStmt.append(") ");
            insertValuesStr.append(")");
            insertStmt.append(insertValuesStr.toString());
            NucleusLogger.DATASTORE_PERSIST.debug("Insert of " + op + " will use statement : " + insertStmt.toString() + " paramValues=" + StringUtils.collectionToString(fieldValues));
            // TODO Create PreparedStatement using statement, bind the values, and execute it

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
            }
            // TODO Handle PK id attributed in datastore (IDENTITY value generation) - retrieve value and set it on the object

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_CASSANDRA.msg("Cassandra.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementInsertCount();
            }
        }
        catch (Exception e) // TODO Change type of this exception to match DataStax
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting object " + op, e);
            throw new NucleusDataStoreException("Exception inserting object for " + op, e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void insertObjects(ObjectProvider... ops)
    {
        // TODO Support bulk insert operations. Currently falls back to one-by-one using superclass
        super.insertObjects(ops);
    }

    public void updateObject(ObjectProvider op, int[] fieldNumbers)
    {
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            AbstractClassMetaData cmd = op.getClassMetaData();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuilder fieldStr = new StringBuilder();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_CASSANDRA.msg("Cassandra.Update.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId(), fieldStr.toString()));
            }

            // TODO Implement UPDATE

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_CASSANDRA.msg("Cassandra.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (Exception e) // TODO Change type of this exception to match DataStax
        {
            NucleusLogger.PERSISTENCE.error("Exception updating object " + op, e);
            throw new NucleusDataStoreException("Exception updating object for " + op, e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void deleteObject(ObjectProvider op)
    {
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_CASSANDRA.msg("Cassandra.Delete.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            // TODO Implement DELETE

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_CASSANDRA.msg("Cassandra.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (Exception e) // TODO Change type of this exception to match DataStax
        {
            NucleusLogger.PERSISTENCE.error("Exception deleting object " + op, e);
            throw new NucleusDataStoreException("Exception deleting object for " + op, e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void deleteObjects(ObjectProvider... ops)
    {
        // TODO Support bulk delete operations. Currently falls back to one-by-one using superclass
        super.deleteObjects(ops);
    }

    public void fetchObject(ObjectProvider op, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = op.getClassMetaData();

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuilder str = new StringBuilder("Fetching object \"");
                str.append(op.getObjectAsPrintable()).append("\" (id=");
                str.append(op.getInternalObjectId()).append(")").append(" fields [");
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        str.append(",");
                    }
                    str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                str.append("]");
                NucleusLogger.DATASTORE_RETRIEVE.debug(str.toString());
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_CASSANDRA.msg("Cassandra.Fetch.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            // TODO Implement FETCH of required fields

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_CASSANDRA.msg("Cassandra.ExecutionTime",
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementFetchCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }

    public void locateObject(ObjectProvider op)
    {
        final AbstractClassMetaData cmd = op.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION || 
            cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                // TODO Implement LOCATE of object
            }
            finally
            {
                mconn.release();
            }
        }
    }

    public void locateObjects(ObjectProvider[] ops)
    {
        // TODO Support bulk locate operations. Currently falls back to one-by-one using superclass
        super.locateObjects(ops);
    }

    public Object findObject(ExecutionContext ec, Object id)
    {
        // This datastore doesn't need to instantiate the objects, so just return null
        return null;
    }
}