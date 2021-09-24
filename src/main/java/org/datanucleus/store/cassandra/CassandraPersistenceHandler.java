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
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.fieldmanager.FetchFieldManager;
import org.datanucleus.store.cassandra.fieldmanager.StoreFieldManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

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

    public void insertObject(DNStateManager sm)
    {
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        AbstractClassMetaData cmd = sm.getClassMetaData();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            CqlSession session = (CqlSession) mconn.getConnection();

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((CassandraStoreManager) storeMgr).manageClasses(new String[]{cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();

            boolean enforceUniquenessInApp = storeMgr.getBooleanProperty(CassandraStoreManager.PROPERTY_CASSANDRA_ENFORCE_UNIQUENESS_IN_APPLICATION);
            if (enforceUniquenessInApp)
            {
                NucleusLogger.DATASTORE_PERSIST.info("User requesting to enforce uniqueness of object identity in their application, so not checking for existence");
            }
            else
            {
                // Check for existence of this object identity in the datastore
                if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    try
                    {
                        locateObject(sm);
                        throw new NucleusUserException(Localiser.msg("Cassandra.Insert.ObjectWithIdAlreadyExists", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
                    }
                    catch (NucleusObjectNotFoundException onfe)
                    {
                    }
                }
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.Insert.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            Object versionValue = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // Process the version value, setting it on the object, and saving the value for the INSERT
                versionValue = ec.getLockManager().getNextVersion(vermd, null);
                if (vermd.getFieldName() != null)
                {
                    // Version is stored in a member, so update the member too
                    sm.replaceField(cmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber(), versionValue);
                }
                sm.setTransactionalVersion(versionValue);
            }

            // Generate the INSERT statement, using cached form if available
            String insertStmt = null;
            // TODO Enable this
            /*
             * if (insertStatementByClassName != null) 
             * { 
             *     insertStmt = insertStatementByClassName.get(cmd.getFullClassName()); 
             * }
             */

            // Use StoreFieldManager to work out the column names and values
            StoreFieldManager storeFM = new StoreFieldManager(sm, true, table);
            sm.provideFields(cmd.getAllMemberPositions(), storeFM);
            Map<String, Object> columnValuesByName = storeFM.getColumnValueByName();

            /*
             * if (insertStmt == null) 
             * {
             */
            // Create the insert statement ("INSERT INTO <schema>.<table> (COL1,COL2,...) VALUES(?,?,...)")
            insertStmt = getInsertStatementForClass(cmd, table, columnValuesByName, ec);

            // Cache the statement
            if (insertStatementByClassName == null)
            {
                insertStatementByClassName = new HashMap<>();
            }
            insertStatementByClassName.put(cmd.getFullClassName(), insertStmt);
            /* } */

            Object discrimValue = cmd.getDiscriminatorValue();

            Object multitenancyValue = null;
            if (table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY) != null)
            {
                // Multitenancy discriminator
                multitenancyValue = ec.getTenantId();
            }
            Object softDeleteValue = null;
            if (table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE) != null)
            {
                softDeleteValue = Boolean.FALSE;
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
            if (softDeleteValue != null)
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
                // TODO Cater for datastore attributed ID
                stmtValues[pos++] = IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId());
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
            if (softDeleteValue != null)
            {
                stmtValues[pos++] = Boolean.FALSE;
            }

            CassandraUtils.logCqlStatement(insertStmt, stmtValues, NucleusLogger.DATASTORE_NATIVE);
            SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
            PreparedStatement stmt = stmtProvider.prepare(insertStmt, session);
            BoundStatement boundStmt = stmt.bind(stmtValues);
            session.execute(boundStmt); // TODO Make use of ResultSet?

            // TODO Handle PK id attributed in datastore (IDENTITY value generation) - retrieve value and set it on the object
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
        }
        catch (DriverException e)
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting object " + sm, e);
            throw new NucleusDataStoreException("Exception inserting object for " + sm, e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method to create the INSERT statement for an object of the specified class. Will add all columns for
     * the class and all superclasses, plus any surrogate datastore-id, version, discriminator. Will have the form
     * <pre>
     * INSERT INTO {schema}.{table} (COL1,COL2,...) VALUES(?,?,...)
     * </pre>
     * All columns are included and if the field is null then at insert CQL will delete the associated cell
     * for the null column.
     * @param cmd Metadata for the class
     * @param table Table used for persistence
     * @param colValuesByName Map of column values keyed by the column name
     * @param ec ExecutionContext
     * @return The INSERT statement
     */
    protected String getInsertStatementForClass(AbstractClassMetaData cmd, Table table, Map<String, Object> colValuesByName, ExecutionContext ec)
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
            insertStmtBuilder.append(table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName());
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
                insertStmtBuilder.append(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
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
            insertStmtBuilder.append(table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR).getName());
            numParams++;
        }

        Column multitenancyCol = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY);
        Object tenantId = ec.getTenantId();
        if (multitenancyCol != null && tenantId != null)
        {
            // Multi-tenancy discriminator
            if (numParams > 0)
            {
                insertStmtBuilder.append(',');
            }
            insertStmtBuilder.append(multitenancyCol.getName());
            numParams++;
        }

        Column softDeleteCol = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE);
        if (softDeleteCol != null)
        {
            // Soft-delete column
            if (numParams > 0)
            {
                insertStmtBuilder.append(',');
            }
            insertStmtBuilder.append(softDeleteCol.getName());
            numParams++;
        }

        insertStmtBuilder.append(") ");

        // Allow user to provide OPTIONS using extensions metadata (comma-separated value, with key='cassandra.insert.using')
        String[] options = cmd.getValuesForExtension(CassandraStoreManager.EXTENSION_CASSANDRA_INSERT_USING);
        if (options != null && options.length > 0)
        {
            insertStmtBuilder.append("USING ");
            for (int i = 0; i < options.length; i++)
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
        for (int i = 0; i < numParams; i++)
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

    public void insertObjects(DNStateManager... sms)
    {
        // TODO Support bulk insert operations. Currently falls back to one-by-one using superclass
        super.insertObjects(sms);
    }

    public void updateObject(DNStateManager sm, int[] fieldNumbers)
    {
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        AbstractClassMetaData cmd = sm.getClassMetaData();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            CqlSession session = (CqlSession) mconn.getConnection();

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((CassandraStoreManager) storeMgr).manageClasses(new String[]{cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();

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
                for (int i = 0; i < fieldNumbers.length; i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.Update.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId(),
                    fieldStr.toString()));
            }

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // Versioned object, so perform optimistic check as required and update version
                Object currentVersion = sm.getTransactionalVersion();
                if (ec.getTransaction().getOptimistic() && cmd.isVersioned())
                {
                    performOptimisticCheck(sm, session, table, vermd, currentVersion);
                }

                Object nextVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                sm.setTransactionalVersion(nextVersion);
                if (vermd.getFieldName() != null)
                {
                    // Version also stored in a field, so update the field value
                    sm.replaceField(cmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber(), nextVersion);
                }
            }

            StoreFieldManager storeFM = new StoreFieldManager(sm, false, table);
            sm.provideFields(fieldNumbers, storeFM);
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
            String[] options = cmd.getValuesForExtension(CassandraStoreManager.EXTENSION_CASSANDRA_UPDATE_USING);
            if (options != null && options.length > 0)
            {
                stmtBuilder.append(" USING ");
                for (int i = 0; i < options.length; i++)
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
                    for (int i = 0; i < fieldNumbers.length; i++)
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
                        setVals.add(sm.getTransactionalVersion());
                    }
                }
                else
                {
                    // Update the stored surrogate value
                    stmtBuilder.append(",").append(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName()).append("=?");
                    Object verVal = sm.getTransactionalVersion();
                    setVals.add(verVal);
                }
            }

            stmtBuilder.append(" WHERE ");
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNums = cmd.getPKMemberPositions();
                for (int i = 0; i < pkFieldNums.length; i++)
                {
                    if (i > 0)
                    {
                        stmtBuilder.append(" AND ");
                    }
                    AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                    // TODO Have PK member with multiple cols
                    Column pkCol = table.getMemberColumnMappingForMember(pkMmd).getColumn(0);
                    stmtBuilder.append(pkCol.getName());
                    stmtBuilder.append("=?");
                    RelationType relType = pkMmd.getRelationType(ec.getClassLoaderResolver());
                    if (RelationType.isRelationSingleValued(relType))
                    {
                        Object pc = sm.provideField(pkFieldNums[i]);
                        setVals.add(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter().getIdForObject(pc)));
                    }
                    else
                    {
                        String cassandraType = pkCol.getTypeName();
                        setVals.add(CassandraUtils.getDatastoreValueForNonPersistableValue(sm.provideField(pkFieldNums[i]), cassandraType, false, ec.getTypeManager(),
                            pkMmd, FieldRole.ROLE_FIELD));
                    }
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                stmtBuilder.append(table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName());
                stmtBuilder.append("=?");
                Object oidVal = IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId());
                setVals.add(CassandraUtils.getDatastoreValueForNonPersistableValue(oidVal, table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getTypeName(), false, ec.getTypeManager(), null, FieldRole.ROLE_FIELD));
            }

            CassandraUtils.logCqlStatement(stmtBuilder.toString(), setVals.toArray(), NucleusLogger.DATASTORE_NATIVE);
            SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
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
            NucleusLogger.PERSISTENCE.error("Exception updating object " + sm, e);
            throw new NucleusDataStoreException("Exception updating object for " + sm, e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void deleteObject(DNStateManager sm)
    {
        assertReadOnlyForUpdateOfObject(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            CqlSession session = (CqlSession) mconn.getConnection();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Cassandra.Delete.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((CassandraStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            if (cmd.isVersioned() && ec.getTransaction().getOptimistic())
            {
                // Versioned object, so perform optimistic check as required and update version
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                Object currentVersion = sm.getTransactionalVersion();
                performOptimisticCheck(sm, session, table, vermd, currentVersion);
            }

            // Invoke any cascade deletion
            sm.loadUnloadedFields();
            sm.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(sm, true));

            Column softDeleteCol = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE);
            if (softDeleteCol != null)
            {
                // SoftDelete : create statement of the form "UPDATE <schema>.<table> SET DELETED=true WHERE KEY1=? (AND KEY2=?)"
                StringBuilder stmtBuilder = new StringBuilder("UPDATE ");
                String schemaName = table.getSchemaName();
                if (schemaName != null)
                {
                    stmtBuilder.append(schemaName).append('.');
                }
                stmtBuilder.append(table.getName());

                List setVals = new ArrayList();
                stmtBuilder.append(" SET ");
                stmtBuilder.append(softDeleteCol.getName()).append("=?");
                setVals.add(Boolean.TRUE);

                stmtBuilder.append(" WHERE ");
                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    int[] pkFieldNums = cmd.getPKMemberPositions();
                    for (int i = 0; i < pkFieldNums.length; i++)
                    {
                        if (i > 0)
                        {
                            stmtBuilder.append(" AND ");
                        }
                        AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                        // TODO Have PK member with multiple cols
                        Column pkCol = table.getMemberColumnMappingForMember(pkMmd).getColumn(0);
                        stmtBuilder.append(pkCol.getName());
                        stmtBuilder.append("=?");
                        RelationType relType = pkMmd.getRelationType(ec.getClassLoaderResolver());
                        if (RelationType.isRelationSingleValued(relType))
                        {
                            Object pc = sm.provideField(pkFieldNums[i]);
                            setVals.add(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter().getIdForObject(pc)));
                        }
                        else
                        {
                            String cassandraType = pkCol.getTypeName();
                            setVals.add(CassandraUtils.getDatastoreValueForNonPersistableValue(sm.provideField(pkFieldNums[i]), cassandraType, false, ec.getTypeManager(),
                                pkMmd, FieldRole.ROLE_FIELD));
                        }
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    stmtBuilder.append(table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName());
                    stmtBuilder.append("=?");
                    Object oidVal = IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId());
                    setVals.add(CassandraUtils.getDatastoreValueForNonPersistableValue(oidVal, table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getTypeName(), false, ec.getTypeManager(), null, FieldRole.ROLE_FIELD));
                }

                CassandraUtils.logCqlStatement(stmtBuilder.toString(), setVals.toArray(), NucleusLogger.DATASTORE_NATIVE);
                SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
                PreparedStatement stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
                session.execute(stmt.bind(setVals.toArray()));
            }
            else
            {
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
                    String[] options = cmd.getValuesForExtension(CassandraStoreManager.EXTENSION_CASSANDRA_DELETE_USING);
                    if (options != null && options.length > 0)
                    {
                        stmtBuilder.append(" USING ");
                        for (int i = 0; i < options.length; i++)
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
                    List<Column> pkCols = getPrimaryKeyColumns(cmd, table, ec.getClassLoaderResolver());
                    int pkColNo = 0;
                    for (Column pkCol : pkCols)
                    {
                        if (pkColNo > 0)
                        {
                            stmtBuilder.append(" AND ");
                        }
                        stmtBuilder.append(pkCol.getName());
                        stmtBuilder.append("=?");
                        pkColNo++;
                    }

                    deleteStmt = stmtBuilder.toString();

                    // Cache the statement
                    if (deleteStatementByClassName == null)
                    {
                        deleteStatementByClassName = new HashMap<>();
                    }
                    deleteStatementByClassName.put(cmd.getFullClassName(), deleteStmt);
                }

                Object[] pkVals = getPkValuesForStatement(sm, table, ec.getClassLoaderResolver());
                CassandraUtils.logCqlStatement(deleteStmt, pkVals, NucleusLogger.DATASTORE_NATIVE);
                SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
                PreparedStatement stmt = stmtProvider.prepare(deleteStmt, session);
                session.execute(stmt.bind(pkVals));
            }

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
            NucleusLogger.PERSISTENCE.error("Exception deleting object " + sm, e);
            throw new NucleusDataStoreException("Exception deleting object for " + sm, e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void deleteObjects(DNStateManager... sms)
    {
        // TODO Support bulk delete operations. Currently falls back to one-by-one using superclass
        super.deleteObjects(sms);
    }

    public void fetchObject(DNStateManager sm, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();

        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            CqlSession session = (CqlSession) mconn.getConnection();

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuilder str = new StringBuilder("Fetching object \"");
                str.append(sm.getObjectAsPrintable()).append("\" (id=");
                str.append(sm.getInternalObjectId()).append(")").append(" fields [");
                for (int i = 0; i < fieldNumbers.length; i++)
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
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("Cassandra.Fetch.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            // Create PreparedStatement and values to bind
            // ("SELECT COL1,COL3,... FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((CassandraStoreManager) storeMgr).manageClasses(new String[]{cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            Set<Integer> nonpersistableFields = null;
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            boolean first = true;
            StringBuilder stmtBuilder = new StringBuilder("SELECT ");
            for (int i = 0; i < fieldNumbers.length; i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                if (mmd.getPersistenceModifier() == FieldPersistenceModifier.PERSISTENT)
                {
                    RelationType relationType = mmd.getRelationType(clr);
                    if (RelationType.isRelationSingleValued(relationType) && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd,
                        relationType, null))
                    {
                        // Embedded PC, so add columns for all fields (and nested fields)
                        List<AbstractMemberMetaData> embMmds = new ArrayList<>();
                        embMmds.add(mmd);
                        List<String> embColNames = new ArrayList<>();
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
                    for (int j = 0; j < mapping.getNumberOfColumns(); j++)
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
                        nonpersistableFields = new HashSet<>();
                    }
                    nonpersistableFields.add(fieldNumbers[i]);
                }
            }

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null && sm.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                if (vermd.getFieldName() != null)
                {
                    // Version stored in a field - check not in the requested fields
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    boolean selected = false;
                    for (int i = 0; i < fieldNumbers.length; i++)
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
                    stmtBuilder.append(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                    first = false;
                }
            }

            if (nonpersistableFields != null)
            {
                // Just go through motions for non-persistable fields
                for (Integer fieldNum : nonpersistableFields)
                {
                    sm.replaceField(fieldNum, sm.provideField(fieldNum));
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

                List<Column> pkCols = getPrimaryKeyColumns(cmd, table, clr);
                int pkColNo = 0;
                for (Column pkCol : pkCols)
                {
                    if (pkColNo > 0)
                    {
                        stmtBuilder.append(" AND ");
                    }
                    stmtBuilder.append(pkCol.getName());
                    stmtBuilder.append("=?");
                    pkColNo++;
                }

                // Add MULTITENANCY if available TODO Support tenancyReadIds
                Column multitenancyCol = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY);
                Object tenantId = null;
                if (multitenancyCol != null)
                {
                    tenantId = ec.getTenantId();
                    if (tenantId != null)
                    {
                        stmtBuilder.append(" AND ");
                        stmtBuilder.append(multitenancyCol.getName());
                        stmtBuilder.append("=?");
                    }
                }

                // Add SOFTDELETE if available
                Column softDeleteCol = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE);
                if (softDeleteCol != null)
                {
                    stmtBuilder.append(" AND ");
                    stmtBuilder.append(softDeleteCol.getName());
                    stmtBuilder.append("=?");
                }

                Object[] pkVals = getPkValuesForStatement(sm, table, clr);
                Object[] stmtParamVals = pkVals;
                if (tenantId != null && multitenancyCol != null)
                {
                    stmtParamVals = new Object[pkVals.length +1];
                    for (int i=0;i<pkVals.length;i++)
                    {
                        stmtParamVals[i] = pkVals[i];
                    }
                    stmtParamVals[pkVals.length] = tenantId;
                }
                if (softDeleteCol != null)
                {
                    stmtParamVals = new Object[pkVals.length +1];
                    for (int i=0;i<pkVals.length;i++)
                    {
                        stmtParamVals[i] = pkVals[i];
                    }
                    stmtParamVals[pkVals.length] = Boolean.FALSE;
                }
                CassandraUtils.logCqlStatement(stmtBuilder.toString(), stmtParamVals, NucleusLogger.DATASTORE_NATIVE);

                SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
                PreparedStatement stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
                ResultSet rs = session.execute(stmt.bind(stmtParamVals));
                Row row = rs.one();
                if (row == null)
                {
                    throw new NucleusObjectNotFoundException("Could not find object with id " + IdentityUtils.getPersistableIdentityForId(sm.getInternalObjectId()));
                }
                FetchFieldManager fetchFM = new FetchFieldManager(sm, row, table);
                if (nonpersistableFields != null)
                {
                    // Strip out any nonpersistable fields
                    int[] persistableFieldNums = new int[fieldNumbers.length - nonpersistableFields.size()];
                    int pos = 0;
                    for (int i = 0; i < fieldNumbers.length; i++)
                    {
                        if (!nonpersistableFields.contains(fieldNumbers[i]))
                        {
                            persistableFieldNums[pos++] = fieldNumbers[i];
                        }
                    }
                    fieldNumbers = persistableFieldNums;
                }
                sm.replaceFields(fieldNumbers, fetchFM);

                if (vermd != null && sm.getTransactionalVersion() == null)
                {
                    // No version set, so retrieve it (note we do this after the retrieval of fields in case just got version)
                    if (vermd.getFieldName() != null)
                    {
                        // Version stored in a field
                        Object datastoreVersion = sm.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
                        sm.setVersion(datastoreVersion);
                    }
                    else
                    {
                        // Surrogate version
                        Column verCol = table.getSurrogateColumn(SurrogateColumnType.VERSION);
                        Object datastoreVersion = verCol.getTypeName().equals("int") ? row.getInt(verCol.getName()) : row.getLong(verCol.getName());
                        sm.setVersion(datastoreVersion);
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
            NucleusLogger.PERSISTENCE.error("Exception fetching object " + sm, e);
            throw new NucleusDataStoreException("Exception fetching object for " + sm, e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Convenience method to populate the "colNames" argument list with column names for the specified
     * embedded field.
     * @param table Table that we are using
     * @param mmds Metadata defining the embedded field (possibly nested, maybe multiple levels).
     * @param colNames List that will have column names added to it
     * @param ec ExecutionContext
     */
    protected void getColumnNamesForEmbeddedMember(Table table, List<AbstractMemberMetaData> mmds, List<String> colNames, ExecutionContext ec)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmds.get(mmds.size() - 1).getType(), clr);
        int[] embFieldNums = embCmd.getAllMemberPositions();
        for (int i = 0; i < embFieldNums.length; i++)
        {
            AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embFieldNums[i]);
            RelationType relationType = embMmd.getRelationType(clr);
            if (RelationType.isRelationSingleValued(relationType) && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, embMmd,
                relationType, mmds.get(mmds.size() - 1)))
            {
                // Nested embedded
                List<AbstractMemberMetaData> embMmds = new ArrayList<>(mmds);
                embMmds.add(embMmd);
                getColumnNamesForEmbeddedMember(table, embMmds, colNames, ec);
                continue;
            }

            List<AbstractMemberMetaData> colMmds = new ArrayList<>(mmds);
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
                NucleusLogger.DATASTORE_SCHEMA.warn("Attempt to find column schema in table=" + table.getName() + " for embedded member at " + strBuilder
                        .toString() + " but not found!" + " Schema generation must be incomplete for this table");
                return;
            }
            for (int j = 0; j < mapping.getNumberOfColumns(); j++)
            {
                Column column = mapping.getColumn(j);
                if (column != null)
                {
                    colNames.add(column.getName());
                }
            }
        }
    }

    public void locateObject(DNStateManager sm)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            try
            {
                CqlSession session = (CqlSession) mconn.getConnection();

                StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                if (sd == null)
                {
                    ((CassandraStoreManager) storeMgr).manageClasses(new String[]{cmd.getFullClassName()}, ec.getClassLoaderResolver(), session);
                    sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                }
                Table table = sd.getTable();

                String locateStmt = null;
                if (locateStatementByClassName != null)
                {
                    locateStmt = locateStatementByClassName.get(cmd.getFullClassName());
                }
                if (locateStmt == null)
                {
                    // Create the locate statement
                    // ("SELECT KEY1(,KEY2) FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
                    StringBuilder stmtBuilder = new StringBuilder("SELECT ");

                    List<Column> pkCols = getPrimaryKeyColumns(cmd, table, ec.getClassLoaderResolver());
                    int pkColNo = 0;
                    for (Column pkCol : pkCols)
                    {
                        if (pkColNo > 0)
                        {
                            stmtBuilder.append(",");
                        }
                        stmtBuilder.append(pkCol.getName());
                        pkColNo++;
                    }

                    stmtBuilder.append(" FROM ");
                    String schemaName = table.getSchemaName();
                    if (schemaName != null)
                    {
                        stmtBuilder.append(schemaName).append('.');
                    }

                    stmtBuilder.append(table.getName()).append(" WHERE ");
                    pkColNo = 0;
                    for (Column pkCol : pkCols)
                    {
                        if (pkColNo > 0)
                        {
                            stmtBuilder.append(" AND ");
                        }
                        stmtBuilder.append(pkCol.getName());
                        stmtBuilder.append("=?");
                        pkColNo++;
                    }
                    // TODO Cater for MULTITENANCY, SOFT_DELETE

                    locateStmt = stmtBuilder.toString();

                    // Cache the statement
                    if (locateStatementByClassName == null)
                    {
                        locateStatementByClassName = new HashMap<>();
                    }
                    locateStatementByClassName.put(cmd.getFullClassName(), locateStmt);
                }

                Object[] pkVals = getPkValuesForStatement(sm, table, ec.getClassLoaderResolver());
                CassandraUtils.logCqlStatement(locateStmt, pkVals, NucleusLogger.DATASTORE_NATIVE);
                SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
                PreparedStatement stmt = stmtProvider.prepare(locateStmt, session);
                ResultSet rs = session.execute(stmt.bind(pkVals));
                Row row = rs.one();
                if (row == null)
                {
                    throw new NucleusObjectNotFoundException();
                }
            }
            catch (DriverException e)
            {
                NucleusLogger.PERSISTENCE.error("Exception locating object " + sm, e);
                throw new NucleusDataStoreException("Exception locating object for " + sm, e);
            }
            finally
            {
                mconn.release();
            }
        }
    }

    public void locateObjects(DNStateManager[] sms)
    {
        // TODO Support bulk locate operations. Currently falls back to one-by-one using superclass
        super.locateObjects(sms);
    }

    public Object findObject(ExecutionContext ec, Object id)
    {
        // This datastore doesn't need to instantiate the objects, so just return null
        return null;
    }

    protected String getVersionStatement(AbstractClassMetaData cmd, Table table, ClassLoaderResolver clr)
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
                col = table.getSurrogateColumn(SurrogateColumnType.VERSION);
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
            List<Column> pkCols = getPrimaryKeyColumns(cmd, table, clr);
            int pkColNo = 0;
            for (Column pkCol : pkCols)
            {
                if (pkColNo > 0)
                {
                    stmtBuilder.append(" AND ");
                }
                stmtBuilder.append(pkCol.getName());
                stmtBuilder.append("=?");
                pkColNo++;
            }

            verStmt = stmtBuilder.toString();

            // Cache the statement
            if (getVersionStatementByClassName == null)
            {
                getVersionStatementByClassName = new HashMap<>();
            }
            getVersionStatementByClassName.put(cmd.getFullClassName(), verStmt);
        }
        return verStmt;
    }

    /**
     * Convenience method to extract the pk values to input into an LOCATE/UPDATE/DELETE/FETCH statement
     * @param sm StateManager we are interested in
     * @param table The table
     * @param clr ClassLoader resolver
     * @return The pk values
     */
    protected Object[] getPkValuesForStatement(DNStateManager sm, Table table, ClassLoaderResolver clr)
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();
        List pkVals = new ArrayList();
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            int[] pkFieldNums = cmd.getPKMemberPositions();
            for (int i = 0; i < pkFieldNums.length; i++)
            {
                AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                RelationType relType = pkMmd.getRelationType(clr);
                Object fieldVal = sm.provideField(pkFieldNums[i]);
                if (relType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(storeMgr.getMetaDataManager(), clr, pkMmd, relType, null))
                {
                    // Embedded : allow 1 level of embedded field for PK
                    DNStateManager embSM = ec.findStateManager(fieldVal);
                    AbstractClassMetaData embCmd = embSM.getClassMetaData();
                    int[] memberPositions = embCmd.getAllMemberPositions();
                    for (int j=0;j<memberPositions.length;j++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[j]);
                        if (embMmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
                        {
                            // Don't need column if not persistent
                            continue;
                        }

                        Object embFieldVal = embSM.provideField(memberPositions[j]);
                        pkVals.add(embFieldVal); // TODO Cater for field mapped to multiple columns
                    }
                }
                else
                {
                    if (RelationType.isRelationSingleValued(relType))
                    {
                        pkVals.add(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter().getIdForObject(fieldVal)));
                    }
                    else
                    {
                        String cassandraType = table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getTypeName();
                        // TODO Cater for field mapped to multiple columns
                        pkVals.add(CassandraUtils.getDatastoreValueForNonPersistableValue(fieldVal, cassandraType, false, storeMgr.getNucleusContext().getTypeManager(), 
                            pkMmd, FieldRole.ROLE_FIELD));
                    }
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            pkVals.add(IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId()));
        }

        return pkVals.toArray();
    }

    protected List<Column> getPrimaryKeyColumns(AbstractClassMetaData cmd, Table table, ClassLoaderResolver clr)
    {
        List<Column> pkCols = new ArrayList<>();
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            int[] pkFieldNums = cmd.getPKMemberPositions();
            for (int i = 0; i < pkFieldNums.length; i++)
            {
                AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]);
                RelationType relType = pkMmd.getRelationType(clr);
                if (relType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(storeMgr.getMetaDataManager(), clr, pkMmd, relType, null))
                {
                    // Embedded : allow 1 level of embedded field for PK
                    List<AbstractMemberMetaData> embMmds = new ArrayList();
                    embMmds.add(pkMmd);

                    AbstractClassMetaData embCmd = storeMgr.getMetaDataManager().getMetaDataForClass(pkMmd.getType(), clr);
                    int[] memberPositions = embCmd.getAllMemberPositions();
                    for (int j=0;j<memberPositions.length;j++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[j]);
                        if (embMmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
                        {
                            // Don't need column if not persistent
                            continue;
                        }

                        embMmds.add(embMmd);
                        MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(embMmds);
                        embMmds.remove(embMmds.size()-1);

                        Column[] cols = mapping.getColumns();
                        for (Column col : cols)
                        {
                            pkCols.add(col);
                        }
                    }
                }
                else
                {
                    MemberColumnMapping mapping = table.getMemberColumnMappingForMember(pkMmd);
                    Column[] cols = mapping.getColumns();
                    for (Column col : cols)
                    {
                        pkCols.add(col);
                    }
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            pkCols.add(table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID));
        }

        return pkCols;
    }

    protected void performOptimisticCheck(DNStateManager sm, CqlSession session, Table table, VersionMetaData vermd, Object currentVersion)
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();

        Object[] pkVals = getPkValuesForStatement(sm, table, sm.getExecutionContext().getClassLoaderResolver());
        String getVersStmt = getVersionStatement(cmd, table, sm.getExecutionContext().getClassLoaderResolver());
        CassandraUtils.logCqlStatement(getVersStmt, pkVals, NucleusLogger.DATASTORE_NATIVE);
        SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
        PreparedStatement stmt = stmtProvider.prepare(getVersStmt, session);
        ResultSet rs = session.execute(stmt.bind(pkVals));
        Row row = rs.one();
        if (row == null)
        {
            // Object doesn't exist
            throw new NucleusDataStoreException("Could not find object with id " + sm.getInternalObjectId() + " in the datastore, so cannot update it");
        }

        String verColName = null;
        if (vermd.getFieldName() == null)
        {
            // Surrogate version
            verColName = table.getSurrogateColumn(SurrogateColumnType.VERSION).getName();
        }
        else
        {
            verColName = table.getMemberColumnMappingForMember(cmd.getMetaDataForMember(vermd.getFieldName())).getColumn(0).getName();
        }

        if (currentVersion instanceof Long)
        {
            long datastoreVersion = row.getLong(verColName);
            if ((Long) currentVersion != datastoreVersion)
            {
                throw new NucleusOptimisticException(
                        "Object " + sm.getInternalObjectId() + " has version=" + datastoreVersion + " in the datastore yet version=" + currentVersion + " in memory");
            }
        }
        else if (currentVersion instanceof Integer)
        {
            int datastoreVersion = row.getInt(verColName);
            if ((Integer) currentVersion != datastoreVersion)
            {
                throw new NucleusOptimisticException(
                        "Object " + sm.getInternalObjectId() + " has version=" + datastoreVersion + " in the datastore yet version=" + currentVersion + " in memory");
            }
        }
    }
}