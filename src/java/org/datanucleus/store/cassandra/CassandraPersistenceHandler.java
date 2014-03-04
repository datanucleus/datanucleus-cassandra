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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.cassandra.fieldmanager.FetchFieldManager;
import org.datanucleus.store.cassandra.fieldmanager.StoreFieldManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * Handler for basic persistence operations with Cassandra datastores.
 */
public class CassandraPersistenceHandler extends AbstractPersistenceHandler
{
    protected static final Localiser LOCALISER_CASSANDRA = Localiser.getInstance(
        "org.datanucleus.store.cassandra.Localisation", CassandraStoreManager.class.getClassLoader());

    protected Map<String, String> insertStatementByClassName;

    protected Map<String, String> deleteStatementByClassName;

    protected Map<String, String> locateStatementByClassName;

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
        AbstractClassMetaData cmd = op.getClassMetaData();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            Session session = (Session)mconn.getConnection();
            if (!storeMgr.managesClass(cmd.getFullClassName()))
            {
                // Make sure schema exists, using this connection
                ((CassandraStoreManager)storeMgr).addClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
            }

            if (!storeMgr.managesClass(cmd.getFullClassName()))
            {
                // Make sure schema exists, using this connection
                ((CassandraStoreManager)storeMgr).addClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
            }
            Table table = (Table) ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getProperty("tableObject");
            // TODO Check for existence? since an INSERT of an existing object in Cassandra is an UPSERT (overwriting the existent object)

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_CASSANDRA.msg("Cassandra.Insert.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            // Generate the INSERT statement, using cached form if available
            String insertStmt = null;
            if (insertStatementByClassName != null)
            {
                insertStmt = insertStatementByClassName.get(cmd.getFullClassName());
            }

            // Use StoreFieldManager to work out the column names and values
            StoreFieldManager storeFM = new StoreFieldManager(op, true, table);
            op.provideFields(cmd.getAllMemberPositions() , storeFM);
            Map<String, Object> columnValuesByName = storeFM.getColumnValueByName();

            if (insertStmt == null)
            {
                // Create the insert statement ("INSERT INTO <schema>.<table> (COL1,COL2,...) VALUES(?,?,...)")
                insertStmt = getInsertStatementForClass(cmd, table, columnValuesByName);

                // Cache the statement
                if (insertStatementByClassName == null)
                {
                    insertStatementByClassName = new HashMap<String, String>();
                }
                insertStatementByClassName.put(cmd.getFullClassName(), insertStmt);
            }

            Object versionValue = null;
            if (cmd.isVersioned())
            {
                // Process the version value, setting it on the object, and saving the value for the INSERT
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    int versionNumber = 1;
                    op.setTransactionalVersion(Long.valueOf(versionNumber));

                    if (vermd.getFieldName() != null)
                    {
                        // Version stored in a member
                        AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                        Object verFieldValue = Long.valueOf(versionNumber);
                        if (verMmd.getType() == int.class || verMmd.getType() == Integer.class)
                        {
                            verFieldValue = Integer.valueOf((int)versionNumber);
                        }
                        op.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
                    }
                    versionValue = versionNumber;
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    Date date = new Date();
                    Timestamp ts = new Timestamp(date.getTime());
                    op.setTransactionalVersion(ts);

                    if (vermd.getFieldName() != null)
                    {
                        // Version stored in a member
                        AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                        op.replaceField(verMmd.getAbsoluteFieldNumber(), ts);
                    }
                    versionValue = ts;
                }
            }

            Object discrimValue = null;
            if (cmd.hasDiscriminatorStrategy())
            {
                // Process the discriminator value, saving the value for the INSERT
                DiscriminatorMetaData discmd = cmd.getDiscriminatorMetaData();
                if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
                {
                    discrimValue = cmd.getFullClassName();
                }
                else
                {
                    discrimValue = discmd.getValue();
                }
            }

            Object multitenancyValue = null;
            if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
            {
                // Multitenancy discriminator
                multitenancyValue = storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID);
            }

            int numValues = columnValuesByName.size();
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                numValues++;
            }
            if (versionValue != null)
            {
                numValues++;
            }
            if (discrimValue != null)
            {
                numValues++;
            }
            if (multitenancyValue != null)
            {
                numValues++;
            }
            Object[] stmtValues = new Object[numValues];
            int pos = 0;
            for (String colName : columnValuesByName.keySet())
            {
                stmtValues[pos++] = columnValuesByName.get(colName);
            }
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                stmtValues[pos++] = ((OID)op.getInternalObjectId()).getKeyValue(); // TODO Cater for datastore attributed ID
            }
            if (versionValue != null)
            {
                stmtValues[pos++] = versionValue;
            }
            if (discrimValue != null)
            {
                stmtValues[pos++] = discrimValue;
            }
            if (multitenancyValue != null)
            {
                stmtValues[pos++] = multitenancyValue;
            }

            CassandraUtils.logCqlStatement(insertStmt, stmtValues, NucleusLogger.DATASTORE_NATIVE);
            SessionStatementProvider stmtProvider = ((CassandraStoreManager)storeMgr).getStatementProvider();
            PreparedStatement stmt = stmtProvider.prepare(insertStmt, session);
            BoundStatement boundStmt = stmt.bind(stmtValues);
            session.execute(boundStmt); // TODO Make use of ResultSet?

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
            }
            // TODO Handle PK id attributed in datastore (IDENTITY value generation) - retrieve value and set it on the object

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_CASSANDRA.msg("Cassandra.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementInsertCount();
            }
        }
        catch (DriverException e)
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting object " + op, e);
            throw new NucleusDataStoreException("Exception inserting object for " + op, e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method to create the INSERT statement for an object of the specified class.
     * Will add all columns for the class and all superclasses, plus any surrogate datastore-id, version, discriminator.
     * Will have the form
     * <pre>INSERT INTO <schema>.<table> (COL1,COL2,...) VALUES(?,?,...)</pre>
     * All columns are included and if the field is null then at insert CQL will delete the associated cell for the null column.
     * @param cmd Metadata for the class
     * @return The INSERT statement
     */
    protected String getInsertStatementForClass(AbstractClassMetaData cmd, Table table, Map<String, Object> colValuesByName)
    {
        StringBuilder insertStmtBuilder = new StringBuilder("INSERT INTO ");
        String schemaName = table.getSchemaName();
        if (schemaName != null)
        {
            insertStmtBuilder.append(schemaName).append('.');
        }
        insertStmtBuilder.append(table.getIdentifier()).append("(");

        int numParams = 0;
        if (colValuesByName != null && !colValuesByName.isEmpty())
        {
            for (String colName : colValuesByName.keySet())
            {
                if (numParams > 0)
                {
                    insertStmtBuilder.append(',');
                }
                insertStmtBuilder.append(colName);
                numParams++;
            }
        }

        if (cmd.getIdentityType() == IdentityType.DATASTORE) // TODO Cater for datastore attributed ID
        {
            if (numParams > 0)
            {
                insertStmtBuilder.append(',');
            }
            insertStmtBuilder.append(table.getDatastoreIdColumn().getIdentifier());
            numParams++;
        }

        if (cmd.isVersioned())
        {
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() == null)
            {
                // Add surrogate version column
                if (numParams > 0)
                {
                    insertStmtBuilder.append(',');
                }
                insertStmtBuilder.append(table.getVersionColumn().getIdentifier());
                numParams++;
            }
        }

        if (cmd.hasDiscriminatorStrategy())
        {
            // Add (surrogate) discriminator column
            if (numParams > 0)
            {
                insertStmtBuilder.append(',');
            }
            insertStmtBuilder.append(table.getDiscriminatorColumn().getIdentifier());
            numParams++;
        }

        if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
        {
            // Multi-tenancy discriminator
            if (numParams > 0)
            {
                insertStmtBuilder.append(',');
            }
            insertStmtBuilder.append(table.getMultitenancyColumn().getIdentifier());
            numParams++;
        }

        insertStmtBuilder.append(") ");
        // TODO Support any USING clauses
        insertStmtBuilder.append("VALUES (");
        for (int i=0;i<numParams;i++)
        {
            if (i > 0)
            {
                insertStmtBuilder.append(',');
            }
            insertStmtBuilder.append('?');
        }
        insertStmtBuilder.append(")");

        return insertStmtBuilder.toString();
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
        AbstractClassMetaData cmd = op.getClassMetaData();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            Session session = (Session)mconn.getConnection();
            if (!storeMgr.managesClass(cmd.getFullClassName()))
            {
                // Make sure schema exists, using this connection
                ((CassandraStoreManager)storeMgr).addClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
            }
            Table table = (Table) ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getProperty("tableObject");

            boolean fieldsToUpdate = false;
            for (int fieldNum : fieldNumbers)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNum);
                if (mmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
                {
                    fieldsToUpdate = true;
                }
            }
            if (!fieldsToUpdate)
            {
                return;
            }

            long startTime = System.currentTimeMillis();
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

            if (cmd.isVersioned())
            {
                // Version object so update the version in the object prior to storing
                Object currentVersion = op.getTransactionalVersion();
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), currentVersion);
                op.setTransactionalVersion(nextVersion);
                if (vermd.getFieldName() != null)
                {
                    // Version also stored in a field, so update the field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);
                }
            }

            StoreFieldManager storeFM = new StoreFieldManager(op, false, table);
            op.provideFields(fieldNumbers , storeFM);
            Map<String, Object> columnValuesByName = storeFM.getColumnValueByName();

            // Create PreparedStatement and values to bind ("UPDATE <schema>.<table> SET COL1=?, COL3=? WHERE KEY1=? (AND KEY2=?)")
            StringBuilder stmtBuilder = new StringBuilder("UPDATE ");
            String schemaName = table.getSchemaName();
            if (schemaName != null)
            {
                stmtBuilder.append(schemaName).append('.');
            }
            stmtBuilder.append(table.getIdentifier());
            // TODO Support any USING clauses

            List setVals = new ArrayList();
            stmtBuilder.append(" SET ");
            if (columnValuesByName != null && !columnValuesByName.isEmpty())
            {
                boolean first = true;
                for (Map.Entry<String, Object> entry : columnValuesByName.entrySet())
                {
                    if (!first)
                    {
                        stmtBuilder.append(',');
                    }
                    stmtBuilder.append(entry.getKey()).append("=?");
                    first = false;
                    setVals.add(entry.getValue());
                }
            }

            if (cmd.isVersioned())
            {
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Update the version field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    boolean updatingVerField = false;
                    for (int i=0;i<fieldNumbers.length;i++)
                    {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber())
                        {
                            updatingVerField = true;
                        }
                    }
                    if (updatingVerField)
                    {
                        stmtBuilder.append(',').append(table.getColumnForMember(verMmd).getIdentifier()).append("=?");
                        setVals.add(op.getTransactionalVersion());
                    }
                }
                else
                {
                    // Update the stored surrogate value
                    stmtBuilder.append(",").append(table.getVersionColumn().getIdentifier()).append("=?");
                    Object verVal = op.getTransactionalVersion();
                    if (verVal instanceof Long)
                    {
                        verVal = Integer.valueOf(((Long)op.getTransactionalVersion()).intValue());
                    }
                    setVals.add(verVal);
                }
            }

            stmtBuilder.append(" WHERE ");
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNums = cmd.getPKMemberPositions();
                for (int i=0;i<pkFieldNums.length;i++)
                {
                    if (i > 0)
                    {
                        stmtBuilder.append(" AND ");
                    }
                    AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                    Column pkCol = table.getColumnForMember(pkMmd);
                    stmtBuilder.append(pkCol.getIdentifier());
                    stmtBuilder.append("=?");
                    RelationType relType = pkMmd.getRelationType(ec.getClassLoaderResolver());
                    if (RelationType.isRelationSingleValued(relType))
                    {
                        Object pc = op.provideField(pkFieldNums[i]);
                        setVals.add(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), ec.getApiAdapter().getIdForObject(pc)));
                    }
                    else
                    {
                        String cassandraType = pkCol.getTypeName();
                        setVals.add(CassandraUtils.getDatastoreValueForNonPersistableValue(op.provideField(pkFieldNums[i]), cassandraType, false, storeMgr.getNucleusContext().getTypeManager()));
                    }
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                stmtBuilder.append(table.getDatastoreIdColumn().getIdentifier());
                stmtBuilder.append("=?");
                Object oidVal = ((OID)op.getInternalObjectId()).getKeyValue(); // TODO Don't hardcode "bigint" (see also CassandraSchemaHandler)
                setVals.add(CassandraUtils.getDatastoreValueForNonPersistableValue(oidVal, "bigint", false, storeMgr.getNucleusContext().getTypeManager()));
            }

            CassandraUtils.logCqlStatement(stmtBuilder.toString(), setVals.toArray(), NucleusLogger.DATASTORE_NATIVE);
            SessionStatementProvider stmtProvider = ((CassandraStoreManager)storeMgr).getStatementProvider();
            PreparedStatement stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
            session.execute(stmt.bind(setVals.toArray()));

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
        catch (DriverException e)
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

        AbstractClassMetaData cmd = op.getClassMetaData();
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

            // Invoke any cascade deletion
            op.loadUnloadedFields();
            op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op, true));

            Table table = (Table) ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getProperty("tableObject");
            String deleteStmt = null;
            if (deleteStatementByClassName != null)
            {
                deleteStmt = deleteStatementByClassName.get(cmd.getFullClassName());
            }
            if (deleteStmt == null)
            {
                // Create the delete statement ("DELETE FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
                StringBuilder stmtBuilder = new StringBuilder("DELETE FROM ");
                String schemaName = table.getSchemaName();
                if (schemaName != null)
                {
                    stmtBuilder.append(schemaName).append('.');
                }
                // TODO Support any USING clauses
                stmtBuilder.append(table.getIdentifier()).append(" WHERE ");

                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    int[] pkFieldNums = cmd.getPKMemberPositions();
                    for (int i=0;i<pkFieldNums.length;i++)
                    {
                        if (i > 0)
                        {
                            stmtBuilder.append(" AND ");
                        }
                        AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                        stmtBuilder.append(table.getColumnForMember(pkMmd).getIdentifier());
                        stmtBuilder.append("=?");
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    stmtBuilder.append(table.getDatastoreIdColumn().getIdentifier());
                    stmtBuilder.append("=?");
                }
                deleteStmt = stmtBuilder.toString();

                // Cache the statement
                if (deleteStatementByClassName == null)
                {
                    deleteStatementByClassName = new HashMap<String, String>();
                }
                deleteStatementByClassName.put(cmd.getFullClassName(), deleteStmt);
            }

            Object[] pkVals = null;
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNums = cmd.getPKMemberPositions();
                pkVals = new Object[pkFieldNums.length];
                for (int i=0;i<pkFieldNums.length;i++)
                {
                    AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                    RelationType relType = pkMmd.getRelationType(ec.getClassLoaderResolver());
                    if (RelationType.isRelationSingleValued(relType))
                    {
                        Object pc = op.provideField(pkFieldNums[i]);
                        pkVals[i] = IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), ec.getApiAdapter().getIdForObject(pc));
                    }
                    else
                    {
                        String cassandraType = table.getColumnForMember(pkMmd).getTypeName();
                        pkVals[i] = CassandraUtils.getDatastoreValueForNonPersistableValue(op.provideField(pkFieldNums[i]), cassandraType, false, storeMgr.getNucleusContext().getTypeManager());
                    }
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                pkVals = new Object[]{((OID)op.getInternalObjectId()).getKeyValue()};
            }

            CassandraUtils.logCqlStatement(deleteStmt, pkVals, NucleusLogger.DATASTORE_NATIVE);
            Session session = (Session)mconn.getConnection();
            SessionStatementProvider stmtProvider = ((CassandraStoreManager)storeMgr).getStatementProvider();
            PreparedStatement stmt = stmtProvider.prepare(deleteStmt, session);
            session.execute(stmt.bind(pkVals));

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
        catch (DriverException e)
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

            // Create PreparedStatement and values to bind ("SELECT COL1,COL3,... FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
            Table table = (Table) ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getProperty("tableObject");
            Set<Integer> nonpersistableFields = null;
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            boolean first = true;
            StringBuilder stmtBuilder = new StringBuilder("SELECT ");
            for (int i=0;i<fieldNumbers.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                if (mmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
                {
                    RelationType relationType = mmd.getRelationType(clr);
                    if (RelationType.isRelationSingleValued(relationType) && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
                    {
                        // Embedded PC, so add columns for all fields (and nested fields)
                        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                        embMmds.add(mmd);
                        List<String> embColNames = new ArrayList<String>();
                        getColumnNamesForEmbeddedMember(embMmds, embColNames, ec);
                        for (String embColName : embColNames)
                        {
                            if (!first)
                            {
                                stmtBuilder.append(',');
                            }
                            stmtBuilder.append(embColName);
                            first = false;
                        }
                        continue;
                    }

                    String colName = table.getColumnForMember(mmd).getIdentifier();
                    if (!first)
                    {
                        stmtBuilder.append(',');
                    }
                    stmtBuilder.append(colName);
                    first = false;
                }
                else
                {
                    if (nonpersistableFields == null)
                    {
                        nonpersistableFields = new HashSet<Integer>();
                    }
                    nonpersistableFields.add(fieldNumbers[i]);
                }
            }

            if (nonpersistableFields != null)
            {
                // Just go through motions for non-persistable fields
                for (Integer fieldNum : nonpersistableFields)
                {
                    op.replaceField(fieldNum, op.provideField(fieldNum));
                }
            }

            if (nonpersistableFields == null || nonpersistableFields.size() != fieldNumbers.length)
            {
                // Some fields needed to be pulled back from the datastore
                stmtBuilder.append(" FROM ");
                String schemaName = table.getSchemaName();
                if (schemaName != null)
                {
                    stmtBuilder.append(schemaName).append('.');
                }
                stmtBuilder.append(table.getIdentifier()).append(" WHERE ");

                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    int[] pkFieldNums = cmd.getPKMemberPositions();
                    for (int i=0;i<pkFieldNums.length;i++)
                    {
                        if (i > 0)
                        {
                            stmtBuilder.append(" AND ");
                        }
                        AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                        stmtBuilder.append(table.getColumnForMember(pkMmd).getIdentifier());
                        stmtBuilder.append("=?");
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    stmtBuilder.append(table.getDatastoreIdColumn().getIdentifier());
                    stmtBuilder.append("=?");
                }
                // TODO Support any USING clauses

                Object[] pkVals = null;
                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    int[] pkFieldNums = cmd.getPKMemberPositions();
                    pkVals = new Object[pkFieldNums.length];
                    for (int i=0;i<pkFieldNums.length;i++)
                    {
                        AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                        RelationType relType = pkMmd.getRelationType(clr);
                        if (RelationType.isRelationSingleValued(relType))
                        {
                            Object pc = op.provideField(pkFieldNums[i]);
                            pkVals[i] = IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), ec.getApiAdapter().getIdForObject(pc));
                        }
                        else
                        {
                            String cassandraType = table.getColumnForMember(pkMmd).getTypeName();
                            pkVals[i] = CassandraUtils.getDatastoreValueForNonPersistableValue(op.provideField(pkFieldNums[i]), cassandraType, false, storeMgr.getNucleusContext().getTypeManager());
                        }
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    pkVals = new Object[]{((OID)op.getInternalObjectId()).getKeyValue()};
                }

                CassandraUtils.logCqlStatement(stmtBuilder.toString(), pkVals, NucleusLogger.DATASTORE_NATIVE);
                Session session = (Session)mconn.getConnection();
                SessionStatementProvider stmtProvider = ((CassandraStoreManager)storeMgr).getStatementProvider();
                PreparedStatement stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
                ResultSet rs = session.execute(stmt.bind(pkVals));
                if (rs.isExhausted())
                {
                    throw new NucleusObjectNotFoundException("Could not find object with id " + op.getInternalObjectId() + " op="+op);
                }

                Row row = rs.one();
                FetchFieldManager fetchFM = new FetchFieldManager(op, row, table);
                if (nonpersistableFields != null)
                {
                    // Strip out any nonpersistable fields
                    int[] persistableFieldNums = new int[fieldNumbers.length - nonpersistableFields.size()];
                    int pos = 0;
                    for (int i=0;i<fieldNumbers.length;i++)
                    {
                        if (!nonpersistableFields.contains(fieldNumbers[i]))
                        {
                            persistableFieldNums[pos++] = fieldNumbers[i];
                        }
                    }
                    fieldNumbers = persistableFieldNums;
                }
                op.replaceFields(fieldNumbers, fetchFM);

                if (cmd.isVersioned() && op.getTransactionalVersion() == null)
                {
                    // No version set, so retrieve it (note we do this after the retrieval of fields in case just got version)
                    VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                    if (vermd.getFieldName() != null)
                    {
                        // Version stored in a field
                        Object datastoreVersion = op.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
                        op.setVersion(datastoreVersion);
                    }
                    else
                    {
                        // Surrogate version
                        Object datastoreVersion = row.getInt(table.getVersionColumn().getIdentifier());
                        op.setVersion(datastoreVersion);
                    }
                }

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
        }
        catch (DriverException e)
        {
            NucleusLogger.PERSISTENCE.error("Exception fetching object " + op, e);
            throw new NucleusDataStoreException("Exception fetching object for " + op, e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Convenience method to populate the "colNames" argument list with column names for the specified embedded field.
     * @param mmds Metadata defining the embedded field (possibly nested, maybe multiple levels).
     * @param colNames List that will have column names added to it
     * @param ec ExecutionContext
     */
    protected void getColumnNamesForEmbeddedMember(List<AbstractMemberMetaData> mmds, List<String> colNames, ExecutionContext ec)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmds.get(mmds.size()-1).getType(), clr);
        int[] embFieldNums = embCmd.getAllMemberPositions();
        for (int i=0;i<embFieldNums.length;i++)
        {
            AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embFieldNums[i]);
            RelationType relationType = embMmd.getRelationType(clr);
            if (RelationType.isRelationSingleValued(relationType) && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, embMmd, relationType, mmds.get(mmds.size()-1)))
            {
                // Nested embedded
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                embMmds.add(embMmd);
                getColumnNamesForEmbeddedMember(embMmds, colNames, ec);
                continue;
            }
            List<AbstractMemberMetaData> colMmds = new ArrayList<AbstractMemberMetaData>(mmds);
            colMmds.add(embMmd);
            String colName = ec.getStoreManager().getNamingFactory().getColumnName(colMmds, 0);
            colNames.add(colName);
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
                String locateStmt = null;
                if (locateStatementByClassName != null)
                {
                    locateStmt = locateStatementByClassName.get(cmd.getFullClassName());
                }
                Table table = (Table) ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getProperty("tableObject");
                if (locateStmt == null)
                {
                    // Create the locate statement ("SELECT KEY1(,KEY2) FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
                    StringBuilder stmtBuilder = new StringBuilder("SELECT ");
                    if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        int[] pkFieldNums = cmd.getPKMemberPositions();
                        for (int i=0;i<pkFieldNums.length;i++)
                        {
                            if (i > 0)
                            {
                                stmtBuilder.append(",");
                            }
                            AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                            stmtBuilder.append(table.getColumnForMember(pkMmd).getIdentifier());
                        }
                    }
                    else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        stmtBuilder.append(table.getDatastoreIdColumn().getIdentifier());
                    }
                    stmtBuilder.append(" FROM ");
                    String schemaName = table.getSchemaName();
                    if (schemaName != null)
                    {
                        stmtBuilder.append(schemaName).append('.');
                    }
                    stmtBuilder.append(table.getIdentifier()).append(" WHERE ");

                    if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        int[] pkFieldNums = cmd.getPKMemberPositions();
                        for (int i=0;i<pkFieldNums.length;i++)
                        {
                            if (i > 0)
                            {
                                stmtBuilder.append(" AND ");
                            }
                            AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                            stmtBuilder.append(table.getColumnForMember(pkMmd).getIdentifier());
                            stmtBuilder.append("=?");
                        }
                    }
                    else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        stmtBuilder.append(table.getDatastoreIdColumn().getIdentifier());
                        stmtBuilder.append("=?");
                    }
                    // TODO Support any USING clauses
                    locateStmt = stmtBuilder.toString();

                    // Cache the statement
                    if (locateStatementByClassName == null)
                    {
                        locateStatementByClassName = new HashMap<String, String>();
                    }
                    locateStatementByClassName.put(cmd.getFullClassName(), locateStmt);
                }

                Object[] pkVals = null;
                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    int[] pkFieldNums = cmd.getPKMemberPositions();
                    pkVals = new Object[pkFieldNums.length];
                    for (int i=0;i<pkFieldNums.length;i++)
                    {
                        AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                        RelationType relType = pkMmd.getRelationType(ec.getClassLoaderResolver());
                        if (RelationType.isRelationSingleValued(relType))
                        {
                            Object pc = op.provideField(pkFieldNums[i]);
                            pkVals[i] = IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), ec.getApiAdapter().getIdForObject(pc));
                        }
                        else
                        {
                            String cassandraType = table.getColumnForMember(pkMmd).getTypeName();
                            pkVals[i] = CassandraUtils.getDatastoreValueForNonPersistableValue(op.provideField(pkFieldNums[i]), cassandraType, false, storeMgr.getNucleusContext().getTypeManager());
                        }
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    pkVals = new Object[]{((OID)op.getInternalObjectId()).getKeyValue()};
                }

                CassandraUtils.logCqlStatement(locateStmt, pkVals, NucleusLogger.DATASTORE_NATIVE);
                Session session = (Session)mconn.getConnection();
                SessionStatementProvider stmtProvider = ((CassandraStoreManager)storeMgr).getStatementProvider();
                PreparedStatement stmt = stmtProvider.prepare(locateStmt, session);
                ResultSet rs = session.execute(stmt.bind(pkVals));
                if (rs.isExhausted())
                {
                    throw new NucleusObjectNotFoundException();
                }
            }
            catch (DriverException e)
            {
                NucleusLogger.PERSISTENCE.error("Exception locating object " + op, e);
                throw new NucleusDataStoreException("Exception locating object for " + op, e);
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