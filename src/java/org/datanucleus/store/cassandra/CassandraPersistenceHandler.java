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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.IdentityType;
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
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.naming.NamingFactory;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Handler for basic persistence operations with Cassandra datastores.
 */
public class CassandraPersistenceHandler extends AbstractPersistenceHandler
{
    protected static final Localiser LOCALISER_CASSANDRA = Localiser.getInstance(
        "org.datanucleus.store.cassandra.Localisation", CassandraStoreManager.class.getClassLoader());

    protected Map<String, String> insertStatementByClassName;

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

            // Generate the INSERT statement, using cached form if available
            String insertStmt = null;
            if (insertStatementByClassName != null)
            {
                insertStmt = insertStatementByClassName.get(cmd.getFullClassName());
            }
            if (insertStmt == null)
            {
                // Create the insert statement ("INSERT INTO <schema>.<table> (COL1,COL2,...) VALUES(?,?,...)") and cache it
                insertStmt = getInsertStatementForClass(cmd);
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
                    long versionNumber = 1;
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

            // Obtain the values to populate the statement with by using StoreFieldManager
            // TODO If we are attributing this object in the datastore and we have relations then do in two steps, so we get the id, persist the other objects, then this side.
            StoreFieldManager storeFM = new StoreFieldManager(op, true);
            op.provideFields(cmd.getAllMemberPositions() , storeFM); // Note that jdoProvideFields in enhancement contract can do these in reverse order TODO Fix enhancement
            Object[] fieldValues = storeFM.getValuesToStore();
            int numValues = fieldValues.length;
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
            Object[] stmtValues;
            if (numValues == fieldValues.length)
            {
                stmtValues = fieldValues;
            }
            else
            {
                stmtValues = new Object[numValues];
                System.arraycopy(fieldValues, 0, stmtValues, 0, fieldValues.length);
                int pos = fieldValues.length;
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
            }

            NucleusLogger.DATASTORE_NATIVE.debug(insertStmt.toString()); // TODO Find a way of putting fieldValues into statement like for RDBMS
            PreparedStatement stmt = session.prepare(insertStmt);
            BoundStatement boundStmt = stmt.bind(stmtValues);
            session.execute(boundStmt); // TODO Make use of ResultSet?

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

    /**
     * Method to create the INSERT statement for an object of the specified class.
     * Will add all columns for the class and all superclasses, plus any surrogate datastore-id, version, discriminator.
     * Will have the form
     * <pre>INSERT INTO <schema>.<table> (COL1,COL2,...) VALUES(?,?,...)</pre>
     * All columns are included and if the field is null then at insert CQL will delete the associated cell for the null column.
     * @param cmd Metadata for the class
     * @return The INSERT statement
     */
    protected String getInsertStatementForClass(AbstractClassMetaData cmd)
    {
        NamingFactory namingFactory = storeMgr.getNamingFactory();
        StringBuilder insertStmtBuilder = new StringBuilder("INSERT INTO ");
        String schemaName = ((CassandraStoreManager)storeMgr).getSchemaNameForClass(cmd);
        if (schemaName != null)
        {
            insertStmtBuilder.append(schemaName).append('.');
        }
        insertStmtBuilder.append(namingFactory.getTableName(cmd)).append("(");

        int numParams = 0;
        int[] memberPositions = cmd.getAllMemberPositions();
        for (int i = 0;i<memberPositions.length;i++)
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
            if (i > 0)
            {
                insertStmtBuilder.append(',');
            }
            insertStmtBuilder.append(namingFactory.getColumnName(mmd, ColumnType.COLUMN));
            numParams++;
        }

        if (cmd.getIdentityType() == IdentityType.DATASTORE) // TODO Cater for datastore attributed ID
        {
            if (numParams > 0)
            {
                insertStmtBuilder.append(',');
            }
            insertStmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.DATASTOREID_COLUMN));
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
                insertStmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.VERSION_COLUMN));
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
            insertStmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN));
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

            // Create PreparedStatement and values to bind ("UPDATE <schema>.<table> SET COL1=?, COL3=? WHERE KEY1=? (AND KEY2=?)")
            NamingFactory namingFactory = storeMgr.getNamingFactory();
            StringBuilder stmtBuilder = new StringBuilder("UPDATE ");
            String schemaName = ((CassandraStoreManager)storeMgr).getSchemaNameForClass(cmd);
            if (schemaName != null)
            {
                stmtBuilder.append(schemaName).append('.');
            }
            stmtBuilder.append(namingFactory.getTableName(cmd));
            // TODO Support any USING clauses

            stmtBuilder.append(" SET ");
            for (int i=0;i<fieldNumbers.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                if (i > 0)
                {
                    stmtBuilder.append(',');
                }
                stmtBuilder.append(namingFactory.getColumnName(mmd, ColumnType.COLUMN));
                stmtBuilder.append("=?");
            }
            Object[] verVals = new Object[0];
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
                        stmtBuilder.append(',').append(namingFactory.getColumnName(verMmd, ColumnType.COLUMN)).append("=?");
                        verVals = new Object[]{op.getTransactionalVersion()};
                    }
                }
                else
                {
                    // Update the stored surrogate value
                    stmtBuilder.append(",").append(namingFactory.getColumnName(cmd, ColumnType.VERSION_COLUMN)).append("=?");
                    verVals = new Object[]{op.getTransactionalVersion()};
                }
            }

            stmtBuilder.append(" WHERE ");
            Object[] pkVals = null;
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNums = cmd.getPKMemberPositions();
                pkVals = new Object[pkFieldNums.length];
                for (int i=0;i<pkFieldNums.length;i++)
                {
                    if (i > 0)
                    {
                        stmtBuilder.append(" AND ");
                    }
                    stmtBuilder.append(namingFactory.getColumnName(cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]), ColumnType.COLUMN));
                    stmtBuilder.append("=?");
                    pkVals[i] = op.provideField(pkFieldNums[i]);
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                stmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.DATASTOREID_COLUMN));
                stmtBuilder.append("=?");
                pkVals = new Object[]{((OID)op.getInternalObjectId()).getKeyValue()};
            }
            NucleusLogger.DATASTORE_NATIVE.debug(stmtBuilder.toString()); // TODO Find a way of putting fieldValues into statement like for RDBMS

            StoreFieldManager storeFM = new StoreFieldManager(op, false);
            op.provideFields(fieldNumbers , storeFM); // Note that jdoProvideFields in enhancement contract can do these in reverse order TODO Fix enhancement
            Object[] setVals = storeFM.getValuesToStore();

            Object[] stmtVals = new Object[setVals.length + pkVals.length + verVals.length];
            System.arraycopy(setVals, 0, stmtVals, 0, setVals.length);
            System.arraycopy(pkVals, 0, stmtVals, setVals.length, pkVals.length);
            System.arraycopy(verVals, 0, stmtVals, setVals.length+pkVals.length, verVals.length);
            Session session = (Session)mconn.getConnection();
            PreparedStatement stmt = session.prepare(stmtBuilder.toString());
            session.execute(stmt.bind(stmtVals));

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

            // Create PreparedStatement and values to bind ("DELETE FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
            NamingFactory namingFactory = storeMgr.getNamingFactory();
            StringBuilder stmtBuilder = new StringBuilder("DELETE FROM ");
            String schemaName = ((CassandraStoreManager)storeMgr).getSchemaNameForClass(cmd);
            if (schemaName != null)
            {
                stmtBuilder.append(schemaName).append('.');
            }
            // TODO Support any USING clauses
            stmtBuilder.append(namingFactory.getTableName(cmd)).append(" WHERE ");

            Object[] pkVals = null;
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNums = cmd.getPKMemberPositions();
                pkVals = new Object[pkFieldNums.length];
                for (int i=0;i<pkFieldNums.length;i++)
                {
                    if (i > 0)
                    {
                        stmtBuilder.append(" AND ");
                    }
                    stmtBuilder.append(namingFactory.getColumnName(cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]), ColumnType.COLUMN));
                    stmtBuilder.append("=?");
                    pkVals[i] = op.provideField(pkFieldNums[i]);
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                stmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.DATASTOREID_COLUMN));
                stmtBuilder.append("=?");
                pkVals = new Object[]{((OID)op.getInternalObjectId()).getKeyValue()};
            }
            NucleusLogger.DATASTORE_NATIVE.debug(stmtBuilder.toString()); // TODO Find a way of putting fieldValues into statement like for RDBMS

            Session session = (Session)mconn.getConnection();
            PreparedStatement stmt = session.prepare(stmtBuilder.toString());
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

            // Create PreparedStatement and values to bind ("SELECT COL1,COL3,... FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
            NamingFactory namingFactory = storeMgr.getNamingFactory();
            StringBuilder stmtBuilder = new StringBuilder("SELECT ");
            for (int i=0;i<fieldNumbers.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                String colName = namingFactory.getColumnName(mmd, ColumnType.COLUMN);
                if (i > 0)
                {
                    stmtBuilder.append(',');
                }
                stmtBuilder.append(colName);
            }
            stmtBuilder.append(" FROM ");
            String schemaName = ((CassandraStoreManager)storeMgr).getSchemaNameForClass(cmd);
            if (schemaName != null)
            {
                stmtBuilder.append(schemaName).append('.');
            }
            stmtBuilder.append(namingFactory.getTableName(cmd)).append(" WHERE ");

            Object[] pkVals = null;
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNums = cmd.getPKMemberPositions();
                pkVals = new Object[pkFieldNums.length];
                for (int i=0;i<pkFieldNums.length;i++)
                {
                    if (i > 0)
                    {
                        stmtBuilder.append(" AND ");
                    }
                    stmtBuilder.append(namingFactory.getColumnName(cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]), ColumnType.COLUMN));
                    stmtBuilder.append("=?");
                    pkVals[i] = op.provideField(pkFieldNums[i]);
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                stmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.DATASTOREID_COLUMN));
                stmtBuilder.append("=?");
                pkVals = new Object[]{((OID)op.getInternalObjectId()).getKeyValue()};
            }
            // TODO Support any USING clauses
            NucleusLogger.DATASTORE_NATIVE.debug(stmtBuilder.toString()); // TODO Find a way of putting fieldValues into statement like for RDBMS

            Session session = (Session)mconn.getConnection();
            PreparedStatement stmt = session.prepare(stmtBuilder.toString());
            ResultSet rs = session.execute(stmt.bind(pkVals));
            if (rs.isExhausted())
            {
                throw new NucleusDataStoreException("Attempt to fetch fields for " + op + " yet no data exists for this object in its table");
            }
            Row row = rs.one();
            FetchFieldManager fetchFM = new FetchFieldManager(op, row);
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
                    String versColName = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
                    Object datastoreVersion = row.getInt(versColName);
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
                // Create PreparedStatement and values to bind ("SELECT KEY1(,KEY2) FROM <schema>.<table> WHERE KEY1=? (AND KEY2=?)")
                NamingFactory namingFactory = storeMgr.getNamingFactory();
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
                        stmtBuilder.append(namingFactory.getColumnName(cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]), ColumnType.COLUMN));
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    stmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.DATASTOREID_COLUMN));
                }
                stmtBuilder.append(" FROM ");
                String schemaName = ((CassandraStoreManager)storeMgr).getSchemaNameForClass(cmd);
                if (schemaName != null)
                {
                    stmtBuilder.append(schemaName).append('.');
                }
                stmtBuilder.append(namingFactory.getTableName(cmd)).append(" WHERE ");

                Object[] pkVals = null;
                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    int[] pkFieldNums = cmd.getPKMemberPositions();
                    pkVals = new Object[pkFieldNums.length];
                    for (int i=0;i<pkFieldNums.length;i++)
                    {
                        if (i > 0)
                        {
                            stmtBuilder.append(" AND ");
                        }
                        stmtBuilder.append(namingFactory.getColumnName(cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[i]), ColumnType.COLUMN));
                        stmtBuilder.append("=?");
                        pkVals[i] = op.provideField(pkFieldNums[i]);
                    }
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    stmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.DATASTOREID_COLUMN));
                    stmtBuilder.append("=?");
                    pkVals = new Object[]{((OID)op.getInternalObjectId()).getKeyValue()};
                }
                // TODO Support any USING clauses
                NucleusLogger.DATASTORE_NATIVE.debug(stmtBuilder.toString()); // TODO Find a way of putting fieldValues into statement like for RDBMS

                Session session = (Session)mconn.getConnection();
                PreparedStatement stmt = session.prepare(stmtBuilder.toString());
                ResultSet rs = session.execute(stmt.bind(pkVals));
                if (rs.isExhausted())
                {
                    throw new NucleusObjectNotFoundException();
                }
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