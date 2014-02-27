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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.naming.NamingFactory;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Handler for schema management with Cassandra.
 */
public class CassandraSchemaHandler extends AbstractStoreSchemaHandler
{
    CassandraStoreManager casStoreMgr;

    public CassandraSchemaHandler(CassandraStoreManager storeMgr)
    {
        super(storeMgr);
        this.casStoreMgr = (CassandraStoreManager)storeMgr;
        // TODO Check if the schema exists and create it according to isAutoCreateSchema()
    }

    /**
     * Method to create a schema (keyspace) in Cassandra.
     * Accepts properties with names "replication", "durable_writes" (case sensitive).
     * @param schemaName Name of the schema
     * @param props Any properties defining the new keyspace
     */
    public void createSchema(String schemaName, Properties props, Object connection)
    {
        // TODO Respect input connection
        ManagedConnection mconn = storeMgr.getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            StringBuilder stmtBuilder = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ");
            stmtBuilder.append(schemaName).append(" WITH ");
            String replicationProp = (props != null ? (String)props.get("replication") : "{'class': 'SimpleStrategy', 'replication_factor' : 3}");
            stmtBuilder.append("replication = ").append(replicationProp);
            if (props != null && props.containsKey("durable_writes"))
            {
                Boolean durable = Boolean.valueOf((String)props.get("durable_writes"));
                if (!durable)
                {
                    stmtBuilder.append(" AND durable_writes=false");
                }
            }

            NucleusLogger.DATASTORE_SCHEMA.debug(stmtBuilder.toString());
            session.execute(stmtBuilder.toString());
            NucleusLogger.DATASTORE_SCHEMA.debug("Schema " + schemaName + " created successfully");
        }
        finally
        {
            mconn.release();
        }
    }

    public void createSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        Session session = (Session)connection;
        String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
        //        String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
        //        boolean completeDdl = (completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true"));

        FileWriter ddlFileWriter = null;
        try
        {
            if (ddlFilename != null)
            {
                // Open the DDL file for writing
                File ddlFile = StringUtils.getFileForFilename(ddlFilename);
                if (ddlFile.exists())
                {
                    // Delete existing file
                    ddlFile.delete();
                }
                if (ddlFile.getParentFile() != null && !ddlFile.getParentFile().exists())
                {
                    // Make sure the directory exists
                    ddlFile.getParentFile().mkdirs();
                }
                ddlFile.createNewFile();
                ddlFileWriter = new FileWriter(ddlFile);

                SimpleDateFormat fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                ddlFileWriter.write("------------------------------------------------------------------\n");
                ddlFileWriter.write("-- DataNucleus SchemaTool " + 
                    "(ran at " + fmt.format(new java.util.Date()) + ")\n");
                ddlFileWriter.write("------------------------------------------------------------------\n");
            }

            ManagedConnection mconn = null;
            try
            {
                if (session == null)
                {
                    mconn = storeMgr.getConnection(-1);
                    session = (Session)mconn.getConnection();
                }

                // Allocate Lists for holding the required CQL statements needed for these classes
                List<String> tableStmts = new ArrayList<String>();
                List<String> constraintStmts = new ArrayList<String>();

                Iterator<String> classIter = classNames.iterator();
                ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
                while (classIter.hasNext())
                {
                    String className = classIter.next();
                    AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                    if (cmd != null)
                    {
                        createSchemaForClass(cmd, session, clr, tableStmts, constraintStmts);
                    }
                }

                if (!tableStmts.isEmpty())
                {
                    // Process the required schema updates for tables
                    for (String stmt : tableStmts)
                    {
                        if (ddlFileWriter == null)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug("Creating table : " + stmt);
                            session.execute(stmt);
                            NucleusLogger.DATASTORE_SCHEMA.debug("Created table successfully");
                        }
                        else
                        {
                            try
                            {
                                ddlFileWriter.write(stmt + ";\n");
                            }
                            catch (IOException ioe)
                            {}
                        }
                    }
                }
                if (!constraintStmts.isEmpty())
                {
                    // Process the required schema updates for constraints
                    for (String stmt : constraintStmts)
                    {
                        if (ddlFileWriter == null)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug("Creating constraint : " + stmt);
                            session.execute(stmt);
                            NucleusLogger.DATASTORE_SCHEMA.debug("Created contraint successfully");
                        }
                        else
                        {
                            try
                            {
                                ddlFileWriter.write(stmt + ";\n");
                            }
                            catch (IOException ioe)
                            {}
                        }
                    }
                }
            }
            finally
            {
                if (mconn != null)
                {
                    mconn.release();
                }
            }
        }
        catch (IOException ioe)
        {
            // Error in writing DDL file
            // TODO Handle this
        }
        finally
        {
            if (ddlFileWriter != null)
            {
                try
                {
                    ddlFileWriter.close();
                }
                catch (IOException ioe)
                {
                    // Error in close of DDL
                }
            }
        }
    }

    /**
     * Method to generate the necessary CQL to create the schema (table/indexes) for the specified class.
     * @param cmd Metadata for the class
     * @param session Session to use for checking of existence in datastore
     * @param clr ClassLoader resolver
     * @param tableStmts List to add any table CQL statements to
     * @param constraintStmts List to add any constraint CQL statements to
     */
    protected void createSchemaForClass(AbstractClassMetaData cmd, Session session, ClassLoaderResolver clr, List<String> tableStmts, List<String> constraintStmts)
    {
        NamingFactory namingFactory = storeMgr.getNamingFactory();
        String schemaNameForClass = casStoreMgr.getSchemaNameForClass(cmd); // Check existence using "select keyspace_name from system.schema_keyspaces where keyspace_name='schema1';"
        String tableName = namingFactory.getTableName(cmd);
        if (cmd.isEmbeddedOnly())
        {
            // No table required here
            return;
        }

        // TODO This is a future way of generating Table definitions so that we can hold a cached data structure for reference in StoreFieldManager/FetchFieldManager
        /*CompleteClassTable theTable = new CompleteClassTable(storeMgr, cmd, new ColumnAttributerImpl(storeMgr, cmd, clr));
        NucleusLogger.GENERAL.info(">> CompleteClassTable - " + theTable);
        List<Column> theTableCols = theTable.getColumns();
        for (Column col : theTableCols)
        {
            AbstractMemberMetaData mmd = col.getMemberMetaData();
            if (mmd != null)
            {
                NucleusLogger.GENERAL.info(">>   " + col.getIdentifier() + " type=" + col.getTypeName() + " mmd=" + mmd.getFullFieldName());
            }
            else
            {
                NucleusLogger.GENERAL.info(">>   " + col.getIdentifier() + " type=" + col.getTypeName());
            }
        }*/

        boolean tableExists = checkTableExistence(session, schemaNameForClass, tableName);

        if (isAutoCreateTables() && !tableExists)
        {
            // Create the table required for this class "CREATE TABLE keyspace.tblName (col1 type1, col2 type2, ...)"
            StringBuilder stmtBuilder = new StringBuilder("CREATE TABLE "); // Note that we could do "IF NOT EXISTS" but have the existence checker method for validation so use that
            if (schemaNameForClass != null)
            {
                stmtBuilder.append(schemaNameForClass).append('.');
            }
            stmtBuilder.append(tableName);
            stmtBuilder.append(" (");
            boolean firstCol = true;

            if (cmd.isVersioned() && cmd.getVersionMetaDataForClass() != null && cmd.getVersionMetaDataForClass().getFieldName() == null)
            {
                // Add column for versioning
                if (!firstCol)
                {
                    stmtBuilder.append(',');
                }
                String cassandraType = "int";
                if (cmd.getVersionMetaDataForClass().getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    cassandraType = "timestamp";
                }
                stmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.VERSION_COLUMN)).append(" ").append(cassandraType);
                firstCol = false;
            }
            if (cmd.hasDiscriminatorStrategy())
            {
                // Add discriminator column
                if (!firstCol)
                {
                    stmtBuilder.append(',');
                }
                stmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.DISCRIMINATOR_COLUMN)).append(" varchar");
                firstCol = false;
            }
            if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
            {
                // Add multitenancy discriminator column
                if (!firstCol)
                {
                    stmtBuilder.append(',');
                }
                stmtBuilder.append(namingFactory.getColumnName(cmd, ColumnType.MULTITENANCY_COLUMN)).append(" varchar");
                firstCol = false;
            }

            // Add columns for managed fields of this class and all superclasses
            int[] memberPositions = cmd.getAllMemberPositions();
            for (int i=0;i<memberPositions.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                RelationType relationType = mmd.getRelationType(clr);
                if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
                {
                    continue;
                }

                if (MetaDataUtils.getInstance().isMemberEmbedded(storeMgr.getMetaDataManager(), clr, mmd, relationType, null))
                {
                    if (RelationType.isRelationSingleValued(relationType))
                    {
                        // Embedded PC field, so add columns for all fields of the embedded
                        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                        embMmds.add(mmd);
                        boolean colAdded = createSchemaForEmbeddedMember(embMmds, clr, stmtBuilder, firstCol, constraintStmts);
                        if (firstCol && colAdded)
                        {
                            firstCol = false;
                        }
                    }
                    else if (RelationType.isRelationMultiValued(relationType))
                    {
                        // Don't support embedded collections
                        NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmd.getFullFieldName() + " is an embedded collection. Not supported so ignoring");
                    }
                }
                else
                {
                    String cassandraType = CassandraUtils.getCassandraColumnTypeForMember(mmd, storeMgr.getNucleusContext().getTypeManager(), clr);
                    if (cassandraType == null)
                    {
                        NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmd.getFullFieldName() + " of type "+ mmd.getTypeName() + " has no supported cassandra type! Ignoring");
                    }
                    else
                    {
                        if (!firstCol)
                        {
                            stmtBuilder.append(',');
                        }
                        stmtBuilder.append(namingFactory.getColumnName(mmd, ColumnType.COLUMN)).append(' ').append(cassandraType);
                    }
                    if (i == 0)
                    {
                        firstCol = false;
                    }
                }

                if (isAutoCreateConstraints())
                {
                    IndexMetaData idxmd = mmd.getIndexMetaData();
                    if (idxmd != null)
                    {
                        // Index specified on this member, so add it TODO Check existence first
                        String colName = namingFactory.getColumnName(mmd, ColumnType.COLUMN);
                        String idxName = namingFactory.getIndexName(mmd, idxmd);
                        String indexStmt = createIndexCQL(idxName, schemaNameForClass, tableName, colName);
                        constraintStmts.add(indexStmt);
                    }
                }
            }

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                if (!firstCol)
                {
                    stmtBuilder.append(',');
                }
                String colName = namingFactory.getColumnName(cmd, ColumnType.DATASTOREID_COLUMN);
                String colType = "bigint"; // TODO Set the type based on jdbc-type of the datastore-id metadata : uuid?, varchar?
                stmtBuilder.append(colName).append(" ").append(colType);

                stmtBuilder.append(",PRIMARY KEY (").append(colName).append(")");
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                if (!firstCol)
                {
                    stmtBuilder.append(',');
                }
                stmtBuilder.append("PRIMARY KEY (");
                int[] pkPositions = cmd.getPKMemberPositions();
                for (int i=0;i<pkPositions.length;i++)
                {
                    if (i > 0)
                    {
                        stmtBuilder.append(',');
                    }
                    AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPositions[i]);
                    stmtBuilder.append(namingFactory.getColumnName(pkMmd, ColumnType.COLUMN));
                }
                stmtBuilder.append(")");
            }

            stmtBuilder.append(')');
            // TODO Add support for "WITH option1=val1 AND option2=val2 ..." by using extensions part of metadata
            tableStmts.add(stmtBuilder.toString());
        }
        else if (tableExists && isAutoCreateColumns())
        {
            // Add/delete any columns to match the current definition (aka "schema evolution")
            // TODO ALTER TABLE schema.table DROP {colName} - Note that this really ought to have a persistence property, and make sure there are no classes sharing the table that need it

            if (cmd.isVersioned() && cmd.getVersionMetaDataForClass() != null && cmd.getVersionMetaDataForClass().getFieldName() == null)
            {
                // TODO Check column for versioning
            }
            if (cmd.hasDiscriminatorStrategy())
            {
                // TODO Check discriminator column
            }
            if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
            {
                // TODO Check multitenancy discriminator column
            }

            // Go through all members for this class (inc superclasses)
            int[] memberPositions = cmd.getAllMemberPositions();
            for (int i=0;i<memberPositions.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                // TODO Check if column exists and ADD if not present  "ALTER TABLE schema.table ADD {colname} {typename}"

                if (isAutoCreateConstraints())
                {
                    IndexMetaData idxmd = mmd.getIndexMetaData();
                    if (idxmd != null)
                    {
                        // Index specified on this member, so add it TODO Check existence first
                        String colName = namingFactory.getColumnName(mmd, ColumnType.COLUMN);
                        String idxName = namingFactory.getIndexName(mmd, idxmd);
                        String indexStmt = createIndexCQL(idxName, schemaNameForClass, tableName, colName);
                        constraintStmts.add(indexStmt);
                    }
                }
            }
        }

        if (isAutoCreateConstraints())
        {
            // Add class-level indexes, including those defined for superclasses (since we hold the fields of those classes too)
            AbstractClassMetaData theCmd = cmd;
            while (theCmd != null)
            {
                IndexMetaData[] clsIdxMds = theCmd.getIndexMetaData();
                if (clsIdxMds != null)
                {
                    for (int i=0;i<clsIdxMds.length;i++)
                    {
                        // TODO Check existence before adding
                        IndexMetaData idxmd = clsIdxMds[i];
                        String[] colNames = idxmd.getColumnNames();
                        if (colNames.length > 1)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.warn("Class " + theCmd.getFullClassName() + " has an index defined with more than 1 column. Cassandra doesn't support composite indexes so ignoring");
                        }
                        else
                        {
                            String idxName = namingFactory.getIndexName(theCmd, idxmd, i);
                            String indexStmt = createIndexCQL(idxName, schemaNameForClass, tableName, colNames[0]);
                            constraintStmts.add(indexStmt);
                        }
                    }
                }
                theCmd = theCmd.getSuperAbstractClassMetaData();
            }

            if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd.getValueForExtension("multitenancy-disable")))
            {
                // TODO Add index on multitenancy discriminator
            }
            // TODO Index on version column? or discriminator?
        }
    }

    /**
     * Method to create the schema (table/indexes) for an embedded member.
     * @param mmds Metadata for the embedded member (last element), and any previous embedded members when this is nested embedded
     * @param clr ClassLoader resolver
     * @param stmtBuilder Builder for the statement to append columns to
     * @param firstCol Whether this will be adding the first column for this table
     * @param constraintStmts List to add any constraint statements to (e.g if this embedded class has indexes)
     * @return whether a column was added
     */
    protected boolean createSchemaForEmbeddedMember(List<AbstractMemberMetaData> mmds, ClassLoaderResolver clr, StringBuilder stmtBuilder, boolean firstCol, List<String> constraintStmts)
    {
        boolean columnAdded = false;

        AbstractMemberMetaData lastMmd = mmds.get(mmds.size()-1);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        MetaDataManager mmgr = storeMgr.getMetaDataManager();
        NamingFactory namingFactory = storeMgr.getNamingFactory();
        AbstractClassMetaData embCmd = mmgr.getMetaDataForClass(lastMmd.getType(), clr);
        int[] memberPositions = embCmd.getAllMemberPositions();
        for (int i=0;i<memberPositions.length;i++)
        {
            AbstractMemberMetaData mmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
            if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
            {
                // Don't need column if not persistent
                continue;
            }
            if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
            {
                // Special case of this being a link back to the owner. TODO Repeat this for nested and their owners
                continue;
            }

            RelationType relationType = mmd.getRelationType(clr);
            if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(mmgr, clr, mmd, relationType, lastMmd))
            {
                if (RelationType.isRelationSingleValued(relationType))
                {
                    // Nested embedded PC, so recurse
                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                    embMmds.add(mmd);
                    boolean added = createSchemaForEmbeddedMember(embMmds, clr, stmtBuilder, firstCol, constraintStmts);
                    if (added)
                    {
                        columnAdded = true;
                        firstCol = false;
                    }
                }
                else
                {
                    // Don't support embedded collections/maps
                    NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmd.getFullFieldName() + " is an embedded collection. Not supported so ignoring");
                }
            }
            else
            {
                String cassandraType = CassandraUtils.getCassandraColumnTypeForMember(mmd, storeMgr.getNucleusContext().getTypeManager(), clr);
                if (cassandraType == null)
                {
                    NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmd.getFullFieldName() + " of type "+ mmd.getTypeName() + " has no supported cassandra type! Ignoring");
                }
                else
                {
                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                    embMmds.add(mmd);
                    String colName = namingFactory.getColumnName(embMmds, 0);
                    if (!firstCol)
                    {
                        stmtBuilder.append(',');
                    }
                    stmtBuilder.append(colName).append(' ').append(cassandraType);
                    columnAdded = true;
                    firstCol = false;
                }
            }
        }
        return columnAdded;
    }

    /**
     * Method to drop a schema (keyspace) in Cassandra.
     * @param schemaName Name of the schema (keyspace).
     * @param props Any properties controlling deletion
     * @param connection Connection to use (null implies this will obtain its own connection)
     */
    public void deleteSchema(String schemaName, Properties props, Object connection)
    {
        // TODO Use input connection
        ManagedConnection mconn = storeMgr.getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            StringBuilder stmtBuilder = new StringBuilder("DROP KEYSPACE IF EXISTS ");
            stmtBuilder.append(schemaName);

            NucleusLogger.DATASTORE_SCHEMA.debug(stmtBuilder.toString());
            session.execute(stmtBuilder.toString());
            NucleusLogger.DATASTORE_SCHEMA.debug("Schema " + schemaName + " dropped successfully");
        }
        finally
        {
            mconn.release();
        }
    }

    public void deleteSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        // TODO Use input connection
        String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
//      String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
//      boolean completeDdl = (completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true"));

        FileWriter ddlFileWriter = null;
        try
        {
            if (ddlFilename != null)
            {
                // Open the DDL file for writing
                File ddlFile = StringUtils.getFileForFilename(ddlFilename);
                if (ddlFile.exists())
                {
                    // Delete existing file
                    ddlFile.delete();
                }
                if (ddlFile.getParentFile() != null && !ddlFile.getParentFile().exists())
                {
                    // Make sure the directory exists
                    ddlFile.getParentFile().mkdirs();
                }
                ddlFile.createNewFile();
                ddlFileWriter = new FileWriter(ddlFile);

                SimpleDateFormat fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                ddlFileWriter.write("------------------------------------------------------------------\n");
                ddlFileWriter.write("-- DataNucleus SchemaTool " + 
                        "(ran at " + fmt.format(new java.util.Date()) + ")\n");
                ddlFileWriter.write("------------------------------------------------------------------\n");
            }

            // TODO Add deletion of any "incrementtable" if used

            NamingFactory namingFactory = storeMgr.getNamingFactory();
            ManagedConnection mconn = storeMgr.getConnection(-1);
            try
            {
                Session session = (Session)mconn.getConnection();

                Iterator<String> classIter = classNames.iterator();
                ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
                while (classIter.hasNext())
                {
                    String className = classIter.next();
                    AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                    if (cmd != null && !cmd.isEmbeddedOnly())
                    {
                        String schemaNameForClass = casStoreMgr.getSchemaNameForClass(cmd); // Check existence using "select keyspace_name from system.schema_keyspaces where keyspace_name='schema1';"
                        String tableName = namingFactory.getTableName(cmd);
                        boolean tableExists = checkTableExistence(session, schemaNameForClass, tableName);
                        if (tableExists)
                        {
                            // Drop any class indexes TODO What about superclass indexMetaData?
                            IndexMetaData[] clsIdxMds = cmd.getIndexMetaData();
                            if (clsIdxMds != null)
                            {
                                for (int i=0;i<clsIdxMds.length;i++)
                                {
                                    IndexMetaData idxmd = clsIdxMds[i];
                                    StringBuilder stmtBuilder = new StringBuilder("DROP INDEX ");
                                    String idxName = namingFactory.getIndexName(cmd, idxmd, i);

                                    if (ddlFileWriter == null)
                                    {
                                        NucleusLogger.DATASTORE_SCHEMA.debug("Dropping index : " + stmtBuilder.toString());
                                        session.execute(stmtBuilder.toString());
                                        NucleusLogger.DATASTORE_SCHEMA.debug("Dropped index " + idxName + " successfully");
                                    }
                                    else
                                    {
                                        try
                                        {
                                            ddlFileWriter.write(stmtBuilder.toString() + ";\n");
                                        }
                                        catch (IOException ioe) {}
                                    }
                                }
                            }
                            // Drop any member-level indexes
                            int[] memberPositions = cmd.getAllMemberPositions();
                            for (int i=0;i<memberPositions.length;i++)
                            {
                                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                                IndexMetaData idxmd = mmd.getIndexMetaData();
                                if (idxmd != null)
                                {
                                    StringBuilder stmtBuilder = new StringBuilder("DROP INDEX ");
                                    String idxName = namingFactory.getIndexName(mmd, idxmd);

                                    if (ddlFileWriter == null)
                                    {
                                        NucleusLogger.DATASTORE_SCHEMA.debug("Dropping index : " + stmtBuilder.toString());
                                        session.execute(stmtBuilder.toString());
                                        NucleusLogger.DATASTORE_SCHEMA.debug("Dropped index " + idxName + " successfully");
                                    }
                                    else
                                    {
                                        try
                                        {
                                            ddlFileWriter.write(stmtBuilder.toString() + ";\n");
                                        }
                                        catch (IOException ioe) {}
                                    }
                                }
                            }

                            // Drop the table
                            StringBuilder stmtBuilder = new StringBuilder("DROP TABLE ");
                            if (schemaNameForClass != null)
                            {
                                stmtBuilder.append(schemaNameForClass).append('.');
                            }
                            stmtBuilder.append(tableName);

                            if (ddlFileWriter == null)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.debug("Dropping table : " + stmtBuilder.toString());
                                session.execute(stmtBuilder.toString());
                                NucleusLogger.DATASTORE_SCHEMA.debug("Dropped table for class " + cmd.getFullClassName() + " successfully");
                            }
                            else
                            {
                                try
                                {
                                    ddlFileWriter.write(stmtBuilder.toString() + ";\n");
                                }
                                catch (IOException ioe) {}
                            }
                        }
                        else
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug("Class " + cmd.getFullClassName() + " table=" + tableName + " didnt exist so can't be dropped");
                        }
                    }
                }
            }
            finally
            {
                mconn.release();
            }
        }
        catch (IOException ioe)
        {
            // Error in writing DDL file
            // TODO Handle this
        }
        finally
        {
            if (ddlFileWriter != null)
            {
                try
                {
                    ddlFileWriter.close();
                }
                catch (IOException ioe)
                {
                    // Error in close of DDL
                }
            }
        }
    }

    public void validateSchema(Set<String> classNames, Properties props, Object connection)
    {
        // TODO Use input connection
        NamingFactory namingFactory = storeMgr.getNamingFactory();
        boolean success = true;
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        ManagedConnection mconn = storeMgr.getConnection(-1);
        try
        {
            Session session = (Session)mconn.getConnection();

            for (String className : classNames)
            {
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd.isEmbeddedOnly())
                {
                    continue;
                }

                String schemaNameForClass = casStoreMgr.getSchemaNameForClass(cmd);
                String tableName = namingFactory.getTableName(cmd);

                boolean tableExists = checkTableExistence(session, schemaNameForClass, tableName);
                if (!tableExists)
                {
                    NucleusLogger.DATASTORE_SCHEMA.error("Table for class " + cmd.getFullClassName() + " doesn't exist : should have name " + tableName + " in schema " + schemaNameForClass);
                    success = false;
                }
                else
                {
                    // Check structure of the table against the required members
                    Map<String, ColumnDetails> colsByName = getColumnDetailsForTable(session, schemaNameForClass, tableName);
                    Set<String> colsFound = new HashSet();
                    int[] memberPositions = cmd.getAllMemberPositions();
                    for (int i=0;i<memberPositions.length;i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                        String columnName = namingFactory.getColumnName(mmd, ColumnType.COLUMN); // TODO Cater for embedded fields
                        ColumnDetails details = colsByName.get(columnName.toLowerCase()); // Stored in lowercase (unless we later on start quoting column names)
                        if (details != null)
                        {
                            String reqdType = CassandraUtils.getCassandraColumnTypeForMember(mmd, storeMgr.getNucleusContext().getTypeManager(), clr);
                            if ((reqdType != null && reqdType.equals(details.typeName)) || (reqdType == null && details.typeName == null))
                            {
                                // Type matches
                            }
                            else
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " column " + columnName + " has type=" + details.typeName + " yet member type " + mmd.getFullFieldName() +
                                    " ought to be using type=" + reqdType);
                            }

                            colsFound.add(columnName.toLowerCase());
                        }
                        else
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " doesn't have column " + columnName + " for member " + mmd.getFullFieldName());
                            success = false;
                        }
                    }
                    // TODO Check datastore id, version, discriminator
                    if (success && colsByName.size() != colsFound.size())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " should have " + colsFound.size() + " columns but has " + colsByName.size() + " columns!");
                        success = false;
                    }

                    // Check class-level indexes TODO What about superclass indexMetaData?
                    IndexMetaData[] clsIdxMds = cmd.getIndexMetaData();
                    if (clsIdxMds != null)
                    {
                        for (int i=0;i<clsIdxMds.length;i++)
                        {
                            IndexMetaData idxmd = clsIdxMds[i];
                            String[] colNames = idxmd.getColumnNames();
                            if (colNames.length == 1)
                            {
                                ColumnDetails details = colsByName.get(colNames[0].toLowerCase());
                                if (details == null || details.indexName == null)
                                {
                                    NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " column=" + colNames[0] + " should have an index but doesn't");
                                }
                            }
                        }
                    }

                    // Add member-level indexes
                    for (int i=0;i<memberPositions.length;i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                        IndexMetaData idxmd = mmd.getIndexMetaData();
                        if (idxmd != null)
                        {
                            String colName = namingFactory.getColumnName(mmd, ColumnType.COLUMN);
                            ColumnDetails details = colsByName.get(colName.toLowerCase());
                            if (details == null || details.indexName == null)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error("Table " + tableName + " column=" + colName + " should have an index but doesn't");
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            mconn.release();
        }

        if (!success)
        {
            throw new NucleusException("Errors were encountered during validation of Cassandra schema");
        }
    }

    protected String createIndexCQL(String indexName, String schemaName, String tableName, String columnName)
    {
        StringBuilder stmtBuilder = new StringBuilder("CREATE INDEX ");
        stmtBuilder.append(indexName);
        stmtBuilder.append(" ON ");
        if (schemaName != null)
        {
            stmtBuilder.append(schemaName).append('.');
        }
        stmtBuilder.append(tableName);
        stmtBuilder.append(" (").append(columnName).append(")");
        return stmtBuilder.toString();
    }

    public static boolean checkTableExistence(Session session, String schemaName, String tableName)
    {
        StringBuilder stmtBuilder = new StringBuilder("SELECT columnfamily_name FROM System.schema_columnfamilies WHERE keyspace_name=? AND columnfamily_name=?");
        NucleusLogger.DATASTORE_SCHEMA.debug("Checking existence of table " + tableName + " using : " + stmtBuilder.toString());
        PreparedStatement stmt = session.prepare(stmtBuilder.toString());
        // TODO What if schema is null?
        ResultSet rs = session.execute(stmt.bind(schemaName.toLowerCase(), tableName.toLowerCase()));
        if (!rs.isExhausted())
        {
            return true;
        }
        return false;
    }

    public Map<String, ColumnDetails> getColumnDetailsForTable(Session session, String schemaName, String tableName)
    {
        StringBuilder stmtBuilder = new StringBuilder("SELECT column_name, index_name, validator FROM system.schema_columns WHERE keyspace_name=? AND columnfamily_name=?");
        NucleusLogger.DATASTORE_SCHEMA.debug("Checking structure of table " + tableName + " using : " + stmtBuilder.toString());
        PreparedStatement stmt = session.prepare(stmtBuilder.toString());
        // TODO What if schema is null?
        ResultSet rs = session.execute(stmt.bind(schemaName.toLowerCase(), tableName.toLowerCase()));
        Map<String, ColumnDetails> cols = new HashMap<String, ColumnDetails>();
        Iterator<Row> iter = rs.iterator();
        while (iter.hasNext())
        {
            Row row = iter.next();
            String typeName = null;
            String validator = row.getString("validator");
            if (validator.indexOf("LongType") >= 0)
            {
                typeName = "bigint";
            }
            else if (validator.indexOf("Int32Type") >= 0)
            {
                typeName = "int";
            }
            else if (validator.indexOf("DoubleType") >= 0)
            {
                typeName = "double";
            }
            else if (validator.indexOf("FloatType") >= 0)
            {
                typeName = "float";
            }
            else if (validator.indexOf("BooleanType") >= 0)
            {
                typeName = "boolean";
            }
            else if (validator.indexOf("UTF8") >= 0)
            {
                typeName = "varchar";
            }
            // TODO Include other types
            String colName = row.getString("column_name");
            ColumnDetails col = new ColumnDetails(colName, row.getString("index_name"), typeName);
            cols.put(colName, col);
        }
        return cols;
    }

    public class ColumnDetails
    {
        String name;
        String indexName;
        String typeName;
        public ColumnDetails(String name, String idxName, String typeName)
        {
            this.name = name;
            this.indexName = idxName;
            this.typeName = typeName;
        }
    }
}