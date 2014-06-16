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
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.cassandra.fieldmanager.FetchFieldManager;
import org.datanucleus.store.cassandra.fieldmanager.StoreFieldManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
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
    protected Map<String, String> insertStatementByClassName;

    protected Map<String, String> deleteStatementByClassName;

    protected Map<String, String> locateStatementByClassName;

    protected Map<String, String> getVersionStatementByClassName;

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
                ((CassandraStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
            }
            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
            // TODO Check for existence? since an INSERT of an existing object in Cassandra is an UPSERT (overwriting the existent object)

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.Insert.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            Object versionValue = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // Process the version value, setting it on the object, and saving the value for the INSERT
                versionValue = VersionHelper.getNextVersion(vermd.getVersionStrategy(), null);
                if (vermd.getFieldName() != null)
                {
                    // Version is stored in a member, so update the member too
                    op.replaceField(cmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber(), versionValue);
                }
                op.setTransactionalVersion(versionValue);
            }

            // Generate the INSERT statement, using cached form if available
            String insertStmt = null;
            // TODO Enable this
            /*if (insertStatementByClassName != null)
            {
                insertStmt = insertStatementByClassName.get(cmd.getFullClassName());
            }*/

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
            if (versionValue != null && vermd != null && vermd.getFieldName() == null)
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
                stmtValues[pos++] = IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId()); // TODO Cater for datastore attributed ID
            }
            if (versionValue != null && vermd != null && vermd.getFieldName() == null)
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
     * <pre>INSERT INTO {schema}.{table} (COL1,COL2,...) VALUES(?,?,...)</pre>
     * All columns are included and if the field is null then at insert CQL will delete the associated cell for the null column.
     * @param cmd Metadata for the class
     * @param table Table used for persistence
     * @param colValuesByName Map of column values keyed by the column name
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
        insertStmtBuilder.append(table.getName()).append("(");

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
            insertStmtBuilder.append(table.getDatastoreIdColumn().getName());
            numParams++;
        }

        VersionMetaData vermd = cmd.getVersionMetaDataForClass();
        if (vermd != null)
        {
            if (vermd.getFieldName() == null)
            {
                // Add surrogate version column
                if (numParams > 0)
                {
                    insertStmtBuilder.append(',');
                }
                insertStmtBuilder.append(table.getVersionColumn().getName());
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
            insertStmtBuilder.append(table.getDiscriminatorColumn().getName());
            numParams++;
        }

        if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
        {
            // Multi-tenancy discriminator
            if (numParams > 0)
            {
                insertStmtBuilder.append(',');
            }
            insertStmtBuilder.append(table.getMultitenancyColumn().getName());
            numParams++;
        }

        insertStmtBuilder.append(") ");

        // Allow user to provide OPTIONS using extensions metadata (comma-separated value, with key='cassandra.insert.using')
        String[] options = cmd.getValuesForExtension("cassandra.insert.using");
        if (options != null && options.length > 0)
        {
            insertStmtBuilder.append("USING ");
            for (int i=0;i<options.length;i++)
            {
                if (i > 0)
                {
                    insertStmtBuilder.append(" AND ");
                }
                insertStmtBuilder.append(options[i]);
            }
            insertStmtBuilder.append(' ');
        }

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
                ((CassandraStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
            }
            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();

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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.Update.Start", op.getObjectAsPrintable(), op.getInternalObjectId(), fieldStr.toString()));
            }

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // Versioned object, so perform optimistic check as required and update version
                Object currentVersion = op.getTransactionalVersion();
                if (ec.getTransaction().getOptimistic() && cmd.isVersioned())
                {
                    performOptimisticCheck(op, session, table, vermd, currentVersion);
                }

                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), currentVersion);
                op.setTransactionalVersion(nextVersion);
                if (vermd.getFieldName() != null)
                {
                    // Version also stored in a field, so update the field value
                    op.replaceField(cmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber(), nextVersion);
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
            stmtBuilder.append(table.getName());

            // Allow user to provide OPTIONS using extensions metadata (comma-separated value, with key='cassandra.update.using')
            String[] options = cmd.getValuesForExtension("cassandra.update.using");
            if (options != null && options.length > 0)
            {
                stmtBuilder.append(" USING ");
                for (int i=0;i<options.length;i++)
                {
                    if (i > 0)
                    {
                        stmtBuilder.append(" AND ");
                    }
                    stmtBuilder.append(options[i]);
                }
            }

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

            if (vermd != null)
            {
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
                            break;
                        }
                    }
                    if (!updatingVerField)
                    {
                        stmtBuilder.append(',').append(table.getMemberColumnMappingForMember(verMmd).getColumn(0).getName()).append("=?");
                        setVals.add(op.getTransactionalVersion());
                    }
                }
                else
                {
                    // Update the stored surrogate value
                    stmtBuilder.append(",").append(table.getVersionColumn().getName()).append("=?");
                    Object verVal = op.getTransactionalVersion();
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
                    Column pkCol = table.getMemberColumnMappingForMember(pkMmd).getColumn(0); // TODO Have PK member with multiple cols
                    stmtBuilder.append(pkCol.getName());
                    stmtBuilder.append("=?");
                    RelationType relType = pkMmd.getRelationType(ec.getClassLoaderResolver());
                    if (RelationType.isRelationSingleValued(relType))
                    {
                        Object pc = op.provideField(pkFieldNums[i]);
                        setVals.add(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter().getIdForObject(pc)));
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
                stmtBuilder.append(table.getDatastoreIdColumn().getName());
                stmtBuilder.append("=?");
                Object oidVal = IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId());
                setVals.add(CassandraUtils.getDatastoreValueForNonPersistableValue(oidVal, table.getDatastoreIdColumn().getTypeName(), false, storeMgr.getNucleusContext().getTypeManager()));
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.Delete.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
            Session session = (Session)mconn.getConnection();
            if (cmd.isVersioned() && ec.getTransaction().getOptimistic())
            {
                // Versioned object, so perform optimistic check as required and update version
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                Object currentVersion = op.getTransactionalVersion();
                performOptimisticCheck(op, session, table, vermd, currentVersion);
            }

            // Invoke any cascade deletion
            op.loadUnloadedFields();
            op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op, true));

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

                // Allow user to provide OPTIONS using extensions metadata (comma-separated value, with key='cassandra.delete.using')
                String[] options = cmd.getValuesForExtension("cassandra.delete.using");
                if (options != null && options.length > 0)
                {
                    stmtBuilder.append(" USING ");
                    for (int i=0;i<options.length;i++)
                    {
                        if (i > 0)
                        {
                            stmtBuilder.append(" AND ");
                        }
                        stmtBuilder.append(options[i]);
                    }
                }

                // WHERE clause
                stmtBuilder.append(table.getName()).append(" WHERE ");
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
                        stmtBuilder.append(table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getName());
                        stmtBuilder.append("=?");
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    stmtBuilder.append(table.getDatastoreIdColumn().getName());
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

            Object[] pkVals = getPkValuesForStatement(op, table);
            CassandraUtils.logCqlStatement(deleteStmt, pkVals, NucleusLogger.DATASTORE_NATIVE);
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
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("Cassandra.Fetch.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            // Create PreparedStatement and values to bind ("SELECT COL1,COL3,... FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
            Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
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
                        getColumnNamesForEmbeddedMember(table, embMmds, embColNames, ec);
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

                    MemberColumnMapping mapping = table.getMemberColumnMappingForMember(mmd);
                    for (int j=0;j<mapping.getNumberOfColumns();j++)
                    {
                        String colName = mapping.getColumn(j).getName();
                        if (!first)
                        {
                            stmtBuilder.append(',');
                        }
                        stmtBuilder.append(colName);
                        first = false;
                    }
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

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null && op.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                if (vermd.getFieldName() != null)
                {
                    // Version stored in a field - check not in the requested fields
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    boolean selected = false;
                    for (int i=0;i<fieldNumbers.length;i++)
                    {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber())
                        {
                            selected = true;
                            break;
                        }
                    }
                    if (!selected)
                    {
                        Column col = table.getMemberColumnMappingForMember(verMmd).getColumn(0);
                        if (!first)
                        {
                            stmtBuilder.append(',');
                        }
                        stmtBuilder.append(col.getName());
                        first = false;
                    }
                }
                else
                {
                    // Surrogate version
                    if (!first)
                    {
                        stmtBuilder.append(',');
                    }
                    stmtBuilder.append(table.getVersionColumn().getName());
                    first = false;
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
                stmtBuilder.append(table.getName()).append(" WHERE ");

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
                        stmtBuilder.append(table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getName());
                        stmtBuilder.append("=?");
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    stmtBuilder.append(table.getDatastoreIdColumn().getName());
                    stmtBuilder.append("=?");
                }

                Object[] pkVals = getPkValuesForStatement(op, table);
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

                if (vermd != null && op.getTransactionalVersion() == null)
                {
                    // No version set, so retrieve it (note we do this after the retrieval of fields in case just got version)
                    if (vermd.getFieldName() != null)
                    {
                        // Version stored in a field
                        Object datastoreVersion = op.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
                        op.setVersion(datastoreVersion);
                    }
                    else
                    {
                        // Surrogate version
                        Column verCol = table.getVersionColumn();
                        Object datastoreVersion = verCol.getTypeName().equals("int") ? row.getInt(verCol.getName()) : row.getLong(verCol.getName());
                        op.setVersion(datastoreVersion);
                    }
                }

                if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("Cassandra.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
     * @param table Table that we are using
     * @param mmds Metadata defining the embedded field (possibly nested, maybe multiple levels).
     * @param colNames List that will have column names added to it
     * @param ec ExecutionContext
     */
    protected void getColumnNamesForEmbeddedMember(Table table, List<AbstractMemberMetaData> mmds, List<String> colNames, ExecutionContext ec)
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
                getColumnNamesForEmbeddedMember(table, embMmds, colNames, ec);
                continue;
            }

            List<AbstractMemberMetaData> colMmds = new ArrayList<AbstractMemberMetaData>(mmds);
            colMmds.add(embMmd);
            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colMmds);
            if (mapping == null)
            {
                StringBuilder strBuilder = new StringBuilder();
                for (AbstractMemberMetaData mmd : mmds)
                {
                    if (strBuilder.length() > 0)
                    {
                        strBuilder.append('.');
                    }
                    strBuilder.append(mmd.getName());
                }
                strBuilder.append('.').append(embMmd.getName());
                NucleusLogger.DATASTORE_SCHEMA.warn("Attempt to find column schema in table=" + table.getName() + " for embedded member at " + strBuilder.toString() + " but not found!" +
                    " Schema generation must be incomplete for this table");
                return;
            }
            for (int j=0;j<mapping.getNumberOfColumns();j++)
            {
                Column column = mapping.getColumn(j);
                if (column != null)
                {
                    colNames.add(column.getName());
                }
            }
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
                Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
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
                            stmtBuilder.append(table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getName());
                        }
                    }
                    else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        stmtBuilder.append(table.getDatastoreIdColumn().getName());
                    }
                    stmtBuilder.append(" FROM ");
                    String schemaName = table.getSchemaName();
                    if (schemaName != null)
                    {
                        stmtBuilder.append(schemaName).append('.');
                    }
                    stmtBuilder.append(table.getName()).append(" WHERE ");

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
                            stmtBuilder.append(table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getName());
                            stmtBuilder.append("=?");
                        }
                    }
                    else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        stmtBuilder.append(table.getDatastoreIdColumn().getName());
                        stmtBuilder.append("=?");
                    }
                    locateStmt = stmtBuilder.toString();

                    // Cache the statement
                    if (locateStatementByClassName == null)
                    {
                        locateStatementByClassName = new HashMap<String, String>();
                    }
                    locateStatementByClassName.put(cmd.getFullClassName(), locateStmt);
                }

                Object[] pkVals = getPkValuesForStatement(op, table);
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

    protected String getVersionStatement(AbstractClassMetaData cmd, Table table)
    {
        String verStmt = null;
        if (getVersionStatementByClassName != null)
        {
            verStmt = getVersionStatementByClassName.get(cmd.getFullClassName());
        }
        if (verStmt == null)
        {
            // Create the version statement ("SELECT VERSION FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
            StringBuilder stmtBuilder = new StringBuilder("SELECT ");
            Column col = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() == null)
            {
                col = table.getVersionColumn();
            }
            else
            {
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                col = table.getMemberColumnMappingForMember(verMmd).getColumn(0);
            }
            stmtBuilder.append(col.getName());
            stmtBuilder.append(" FROM ");
            String schemaName = table.getSchemaName();
            if (schemaName != null)
            {
                stmtBuilder.append(schemaName).append('.');
            }
            stmtBuilder.append(table.getName()).append(" WHERE ");

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
                    stmtBuilder.append(table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getName());
                    stmtBuilder.append("=?");
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                stmtBuilder.append(table.getDatastoreIdColumn().getName());
                stmtBuilder.append("=?");
            }
            verStmt = stmtBuilder.toString();

            // Cache the statement
            if (getVersionStatementByClassName == null)
            {
                getVersionStatementByClassName = new HashMap<String, String>();
            }
            getVersionStatementByClassName.put(cmd.getFullClassName(), verStmt);
        }
        return verStmt;
    }

    /**
     * Convenience method to extract the pk values to input into an LOCATE/UPDATE/DELETE/FETCH statement
     * @param op ObjectProvider we are interested in
     * @param table The table
     * @return The pk values
     */
    protected Object[] getPkValuesForStatement(ObjectProvider op, Table table)
    {
        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
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
                    pkVals[i] = IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter().getIdForObject(pc));
                }
                else
                {
                    String cassandraType = table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getTypeName();
                    pkVals[i] = CassandraUtils.getDatastoreValueForNonPersistableValue(op.provideField(pkFieldNums[i]), cassandraType, false, storeMgr.getNucleusContext().getTypeManager());
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            pkVals = new Object[]{IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId())};
        }

        return pkVals;
    }

    protected void performOptimisticCheck(ObjectProvider op, Session session, Table table, VersionMetaData vermd, Object currentVersion)
    {
        AbstractClassMetaData cmd = op.getClassMetaData();

        Object[] pkVals = getPkValuesForStatement(op, table);
        String getVersStmt = getVersionStatement(cmd, table);
        CassandraUtils.logCqlStatement(getVersStmt, pkVals, NucleusLogger.DATASTORE_NATIVE);
        SessionStatementProvider stmtProvider = ((CassandraStoreManager)storeMgr).getStatementProvider();
        PreparedStatement stmt = stmtProvider.prepare(getVersStmt, session);
        ResultSet rs = session.execute(stmt.bind(pkVals));
        if (rs.isExhausted())
        {
            // Object doesn't exist
            throw new NucleusDataStoreException("Could not find object with id " + op.getInternalObjectId() + " in the datastore, so cannot update it");
        }

        Row row = rs.one();
        String verColName = null;
        if (vermd.getFieldName() == null)
        {
            // Surrogate version
            verColName = table.getVersionColumn().getName();
        }
        else
        {
            AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
            verColName = table.getMemberColumnMappingForMember(verMmd).getColumn(0).getName();
        }
        if (currentVersion instanceof Long)
        {
            long datastoreVersion = row.getLong(verColName);
            if ((Long)currentVersion != datastoreVersion)
            {
                throw new NucleusOptimisticException("Object " + op.getInternalObjectId() + " has version=" + datastoreVersion + " in the datastore yet version=" + currentVersion + " in memory");
            }
        }
        else if (currentVersion instanceof Integer)
        {
            int datastoreVersion = row.getInt(verColName);
            if ((Integer)currentVersion != datastoreVersion)
            {
                throw new NucleusOptimisticException("Object " + op.getInternalObjectId() + " has version=" + datastoreVersion + " in the datastore yet version=" + currentVersion + " in memory");
            }
        }
    }
}