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
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.naming.NamingFactory;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Handler for schema management with Cassandra. Supports the following metadata extensions
 * <ul>
 * <li>ClassMetaData extension "cassandra.createTable.options" specifies any OPTIONS for a CREATE TABLE
 * statement (comma-separated value if wanting multiple options)</li>
 * <li>IndexMetaData extension "cassandra.createIndex.using" specifies any USING clause for a CREATE INDEX
 * statement</li>
 * </ul>
 */
public class CassandraSchemaHandler extends AbstractStoreSchemaHandler
{
    public CassandraSchemaHandler(CassandraStoreManager storeMgr)
    {
        super(storeMgr);
        // TODO Check if the schema exists and create it according to isAutoCreateSchema()
    }

    /**
     * Method to create a schema (keyspace) in Cassandra. Accepts properties with names "replication",
     * "durable_writes" (case sensitive).
     * @param schemaName Name of the schema
     * @param props Any properties defining the new keyspace
     */
    public void createSchema(String schemaName, Properties props, Object connection)
    {
        Session session = (Session) connection;
        ManagedConnection mconn = null;
        try
        {
            if (session == null)
            {
                mconn = storeMgr.getConnection(-1);
                session = (Session) mconn.getConnection();
            }

            StringBuilder stmtBuilder = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ");
            stmtBuilder.append(schemaName).append(" WITH ");
            String replicationProp = (props != null ? (String) props.get("replication") : "{'class': 'SimpleStrategy', 'replication_factor' : 3}");
            stmtBuilder.append("replication = ").append(replicationProp);
            if (props != null && props.containsKey("durable_writes"))
            {
                Boolean durable = Boolean.valueOf((String) props.get("durable_writes"));
                if (!durable)
                {
                    stmtBuilder.append(" AND durable_writes=false");
                }
            }

            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.CreateSchema", stmtBuilder.toString()));
            session.execute(stmtBuilder.toString());
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.CreateSchema.Success"));
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }
    }

    public void createSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        Session session = (Session) connection;
        String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
        // String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
        // boolean completeDdl = (completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true"));

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
                ddlFileWriter.write("-- DataNucleus SchemaTool " + "(ran at " + fmt.format(new java.util.Date()) + ")\n");
                ddlFileWriter.write("------------------------------------------------------------------\n");
            }

            ManagedConnection mconn = null;
            try
            {
                if (session == null)
                {
                    mconn = storeMgr.getConnection(-1);
                    session = (Session) mconn.getConnection();
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
                        try
                        {
                            createSchemaForClass(cmd, session, clr, tableStmts, constraintStmts);
                        }
                        catch (Exception e)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.error("Could not create schema for class=" + cmd.getFullClassName() + " - see the nested exception", e);
                            // TODO Throw an exception
                        }
                    }
                }

                if (!tableStmts.isEmpty())
                {
                    // Process the required schema updates for tables
                    for (String stmt : tableStmts)
                    {
                        if (ddlFileWriter == null)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.CreateTable", stmt));
                            session.execute(stmt);
                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.CreateTable.Success"));
                        }
                        else
                        {
                            try
                            {
                                ddlFileWriter.write(stmt + ";\n");
                            }
                            catch (IOException ioe)
                            {
                            }
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
                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.CreateConstraint", stmt));
                            session.execute(stmt);
                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.CreateConstraint.Success"));
                        }
                        else
                        {
                            try
                            {
                                ddlFileWriter.write(stmt + ";\n");
                            }
                            catch (IOException ioe)
                            {
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                NucleusLogger.GENERAL.error("Exception in schema generation", e);
                throw e;
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
    protected void createSchemaForClass(AbstractClassMetaData cmd, Session session, ClassLoaderResolver clr, List<String> tableStmts,
            List<String> constraintStmts)
    {
        if (cmd.isEmbeddedOnly() || cmd.getPersistenceModifier() != ClassPersistenceModifier.PERSISTENCE_CAPABLE)
        {
            // No table required here
            return;
        }
        if (cmd instanceof ClassMetaData && ((ClassMetaData) cmd).isAbstract())
        {
            // No table required here
            return;
        }

        StoreData storeData = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        Table table = null;
        if (storeData != null)
        {
            table = storeData.getTable();
        }
        else
        {
            table = new CompleteClassTable(storeMgr, cmd, new SchemaVerifierImpl(storeMgr, cmd, clr));
        }
        // TODO Check existence of schema using
        // "select keyspace_name from system.schema_keyspaces where keyspace_name='schema1';"
        String schemaName = table.getSchemaName();

        SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
        if (checkTableExistence(session, stmtProvider, schemaName, table.getName()))
        {
            // Add/delete any columns/constraints to match the current definition (aka "schema evolution")
            Map<String, ColumnDetails> tableStructure = getColumnDetailsForTable(session, stmtProvider, schemaName, table.getName());

            if (autoCreateColumns)
            {
                // Add any missing columns
                List<Column> columns = table.getColumns();
                for (Column column : columns)
                {
                    ColumnDetails colDetails = getColumnDetailsForColumn(column, tableStructure);
                    if (colDetails == null)
                    {
                        // Add column since doesn't exist
                        StringBuilder stmtBuilder = new StringBuilder("ALTER TABLE ");
                        if (schemaName != null)
                        {
                            stmtBuilder.append(schemaName).append('.');
                        }
                        stmtBuilder.append(table.getName());
                        stmtBuilder.append(" ADD COLUMN ");
                        stmtBuilder.append(column.getName()).append(" ").append(column.getTypeName());
                        tableStmts.add(stmtBuilder.toString());
                    }
                    else
                    {
                        if (colDetails.typeName != null && !colDetails.typeName.equals(column.getTypeName()))
                        {
                            // TODO Change the column type if requested. What about existing data
                            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.TableColumnTypeIncorrect", table.getName(), column.getName(),
                                colDetails.typeName, column.getTypeName()));
                        }
                    }
                }
            }

            if (autoDeleteColumns)
            {
                // Delete any columns that are not needed by this class
                Iterator<Map.Entry<String, ColumnDetails>> tableStructureIter = tableStructure.entrySet().iterator();
                while (tableStructureIter.hasNext())
                {
                    Map.Entry<String, ColumnDetails> entry = tableStructureIter.next();
                    String colName = entry.getKey();

                    Column col = table.getColumnForName(colName);
                    if (col == null)
                    {
                        // Add column since doesn't exist
                        StringBuilder stmtBuilder = new StringBuilder("ALTER TABLE ");
                        if (schemaName != null)
                        {
                            stmtBuilder.append(schemaName).append('.');
                        }
                        stmtBuilder.append(table.getName());
                        stmtBuilder.append(" DROP COLUMN ");
                        stmtBuilder.append(colName); // Needs quoting?
                        tableStmts.add(stmtBuilder.toString());
                    }
                }
            }

            if (isAutoCreateConstraints())
            {
                // Class-level indexes
                NamingFactory namingFactory = storeMgr.getNamingFactory();
                AbstractClassMetaData theCmd = cmd;
                while (theCmd != null)
                {
                    IndexMetaData[] clsIdxMds = theCmd.getIndexMetaData();
                    if (clsIdxMds != null)
                    {
                        for (int i = 0; i < clsIdxMds.length; i++)
                        {
                            IndexMetaData idxmd = clsIdxMds[i];
                            String[] colNames = idxmd.getColumnNames();
                            if (colNames.length > 1)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexForClassWithMultipleColumns",
                                    theCmd.getFullClassName()));
                            }
                            else
                            {
                                ColumnDetails colDetails = tableStructure.get(colNames[0]);
                                String idxName = namingFactory.getConstraintName(theCmd, idxmd, i);
                                if (colDetails == null)
                                {
                                    // Add index
                                    String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), colNames[0], idxmd);
                                    constraintStmts.add(indexStmt);
                                }
                                else
                                {
                                    if (!idxName.equals(colDetails.indexName))
                                    {
                                        NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, colDetails.indexName));
                                    }
                                }
                            }
                        }
                    }
                    // TODO Support unique constraints?
                    theCmd = theCmd.getSuperAbstractClassMetaData();
                }

                // Column-level indexes
                Set<MemberColumnMapping> mappings = table.getMemberColumnMappings();
                for (MemberColumnMapping mapping : mappings)
                {
                    IndexMetaData idxmd = mapping.getMemberMetaData().getIndexMetaData();
                    if (idxmd != null)
                    {
                        String[] colNames = idxmd.getColumnNames();
                        if (colNames.length > 1)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexForMemberWithMultipleColumns", mapping.getMemberMetaData()
                                    .getFullFieldName()));
                        }
                        else
                        {
                            if (mapping.getNumberOfColumns() == 1)
                            {
                                Column column = mapping.getColumn(0);
                                ColumnDetails colDetails = getColumnDetailsForColumn(column, tableStructure);
                                String idxName = namingFactory.getConstraintName(mapping.getMemberMetaData(), idxmd);
                                if (colDetails == null)
                                {
                                    // Add index
                                    String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), idxmd);
                                    constraintStmts.add(indexStmt);
                                }
                                else
                                {
                                    if (!idxName.equals(colDetails.indexName))
                                    {
                                        NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, colDetails.indexName));
                                    }
                                }
                            }
                        }
                    }
                }

                if (cmd.isVersioned() && cmd.getVersionMetaDataForClass() != null && cmd.getVersionMetaDataForClass().getFieldName() == null)
                {
                    VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                    if (vermd.getIndexMetaData() != null)
                    {
                        Column column = table.getVersionColumn();
                        ColumnDetails colDetails = getColumnDetailsForColumn(column, tableStructure);
                        if (colDetails == null)
                        {
                            String idxName = namingFactory.getConstraintName(cmd, vermd.getIndexMetaData(), ColumnType.VERSION_COLUMN);
                            String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), vermd.getIndexMetaData());
                            constraintStmts.add(indexStmt);
                        }
                        else
                        {
                            String idxName = namingFactory.getConstraintName(cmd, vermd.getIndexMetaData(), ColumnType.VERSION_COLUMN);
                            if (!idxName.equals(colDetails.indexName))
                            {
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, colDetails.indexName));
                            }
                        }
                    }
                }
                if (cmd.hasDiscriminatorStrategy())
                {
                    DiscriminatorMetaData dismd = cmd.getDiscriminatorMetaData();
                    if (dismd.getIndexMetaData() != null)
                    {
                        Column column = table.getDiscriminatorColumn();
                        ColumnDetails colDetails = getColumnDetailsForColumn(column, tableStructure);
                        if (colDetails == null)
                        {
                            String idxName = namingFactory.getConstraintName(cmd, dismd.getIndexMetaData(), ColumnType.DISCRIMINATOR_COLUMN);
                            String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), dismd.getIndexMetaData());
                            constraintStmts.add(indexStmt);
                        }
                        else
                        {
                            String idxName = namingFactory.getConstraintName(cmd, dismd.getIndexMetaData(), ColumnType.DISCRIMINATOR_COLUMN);
                            if (!idxName.equals(colDetails.indexName))
                            {
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, colDetails.indexName));
                            }
                        }
                    }
                }
                if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd
                        .getValueForExtension("multitenancy-disable")))
                {
                    // TODO Add index on multitenancy discriminator
                }
            }
        }
        else
        {
            if (isAutoCreateTables())
            {
                // Create the table required for this class
                // "CREATE TABLE keyspace.tblName (col1 type1, col2 type2, ...)"
                // Note that we could do "IF NOT EXISTS" but have the existence checker method for validation so use that
                StringBuilder stmtBuilder = new StringBuilder("CREATE TABLE ");
                if (schemaName != null)
                {
                    stmtBuilder.append(schemaName).append('.');
                }
                stmtBuilder.append(table.getName());
                stmtBuilder.append(" (");

                List<String> pkColNames = new ArrayList<String>();
                for (Column column : table.getColumns())
                {
                    stmtBuilder.append(column.getName()).append(' ').append(column.getTypeName()).append(',');
                    if (column.isPrimaryKey())
                    {
                        pkColNames.add(column.getName());
                    }
                }

                stmtBuilder.append(" PRIMARY KEY (");
                Iterator<String> pkColNameIter = pkColNames.iterator();
                while (pkColNameIter.hasNext())
                {
                    stmtBuilder.append(pkColNameIter.next());
                    if (pkColNameIter.hasNext())
                    {
                        stmtBuilder.append(',');
                    }
                }
                stmtBuilder.append(")");

                stmtBuilder.append(")");

                // Allow user to provide OPTIONS using extensions metadata (comma-separated value, with
                // key='cassandra.createTable.options')
                String[] options = cmd.getValuesForExtension("cassandra.createTable.options");
                if (options != null && options.length > 0)
                {
                    stmtBuilder.append(" WITH ");
                    for (int i = 0; i < options.length; i++)
                    {
                        if (i > 0)
                        {
                            stmtBuilder.append(" AND ");
                        }
                        stmtBuilder.append(options[i]);
                    }
                }

                tableStmts.add(stmtBuilder.toString());
            }

            if (isAutoCreateConstraints())
            {
                NamingFactory namingFactory = storeMgr.getNamingFactory();

                // Add class-level indexes, including those defined for superclasses (since we hold the fields
                // of those classes too)
                AbstractClassMetaData theCmd = cmd;
                while (theCmd != null)
                {
                    IndexMetaData[] clsIdxMds = theCmd.getIndexMetaData();
                    if (clsIdxMds != null)
                    {
                        for (int i = 0; i < clsIdxMds.length; i++)
                        {
                            IndexMetaData idxmd = clsIdxMds[i];
                            String[] colNames = idxmd.getColumnNames();
                            if (colNames.length > 1)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexForClassWithMultipleColumns",
                                    theCmd.getFullClassName()));
                            }
                            else
                            {
                                String idxName = namingFactory.getConstraintName(theCmd, idxmd, i);
                                String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), colNames[0], idxmd);
                                constraintStmts.add(indexStmt);
                            }
                        }
                    }
                    theCmd = theCmd.getSuperAbstractClassMetaData();
                }

                // Add column-level indexes
                Set<MemberColumnMapping> mappings = table.getMemberColumnMappings();
                for (MemberColumnMapping mapping : mappings)
                {
                    AbstractMemberMetaData mmd = mapping.getMemberMetaData();
                    IndexMetaData idxmd = mmd.getIndexMetaData();
                    if (idxmd != null)
                    {
                        if (mapping.getNumberOfColumns() == 1)
                        {
                            // Index specified on this member, so add it TODO Add check if member has multiple
                            // columns
                            String idxName = namingFactory.getConstraintName(mmd, idxmd);
                            String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), mapping.getColumn(0).getName(), idxmd);
                            constraintStmts.add(indexStmt);
                        }
                    }
                }

                if (cmd.isVersioned() && cmd.getVersionMetaDataForClass() != null && cmd.getVersionMetaDataForClass().getFieldName() == null)
                {
                    VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                    if (vermd.getIndexMetaData() != null)
                    {
                        Column column = table.getVersionColumn();
                        String idxName = namingFactory.getConstraintName(cmd, vermd.getIndexMetaData(), ColumnType.VERSION_COLUMN);
                        String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), vermd.getIndexMetaData());
                        constraintStmts.add(indexStmt);
                    }
                }
                if (cmd.hasDiscriminatorStrategy())
                {
                    DiscriminatorMetaData dismd = cmd.getDiscriminatorMetaData();
                    if (dismd.getIndexMetaData() != null)
                    {
                        Column column = table.getDiscriminatorColumn();
                        String idxName = namingFactory.getConstraintName(cmd, dismd.getIndexMetaData(), ColumnType.DISCRIMINATOR_COLUMN);
                        String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), dismd.getIndexMetaData());
                        constraintStmts.add(indexStmt);
                    }
                }
                if (storeMgr.getStringProperty(PropertyNames.PROPERTY_MAPPING_TENANT_ID) != null && !"true".equalsIgnoreCase(cmd
                        .getValueForExtension("multitenancy-disable")))
                {
                    Column column = table.getMultitenancyColumn();
                    String idxName = cmd.getName() + "_TENANCY_IDX";
                    String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), null);
                    constraintStmts.add(indexStmt);
                }
            }
        }
    }

    /**
     * Method to drop a schema (keyspace) in Cassandra.
     * @param schemaName Name of the schema (keyspace).
     * @param props Any properties controlling deletion
     * @param connection Connection to use (null implies this will obtain its own connection)
     */
    public void deleteSchema(String schemaName, Properties props, Object connection)
    {
        Session session = (Session) connection;
        ManagedConnection mconn = null;
        try
        {
            if (session == null)
            {
                mconn = storeMgr.getConnection(-1);
                session = (Session) mconn.getConnection();
            }

            StringBuilder stmtBuilder = new StringBuilder("DROP KEYSPACE IF EXISTS ");
            stmtBuilder.append(schemaName);

            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.DropSchema", stmtBuilder.toString()));
            session.execute(stmtBuilder.toString());
            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.DropSchema", schemaName));
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }
    }

    public void deleteSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        Session session = (Session) connection;
        String ddlFilename = props != null ? props.getProperty("ddlFilename") : null;
        // String completeDdlProp = props != null ? props.getProperty("completeDdl") : null;
        // boolean completeDdl = (completeDdlProp != null && completeDdlProp.equalsIgnoreCase("true"));

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
                ddlFileWriter.write("-- DataNucleus SchemaTool " + "(ran at " + fmt.format(new java.util.Date()) + ")\n");
                ddlFileWriter.write("------------------------------------------------------------------\n");
            }

            // TODO Add deletion of any "incrementtable" if used

            ManagedConnection mconn = null;
            try
            {
                if (session == null)
                {
                    mconn = storeMgr.getConnection(-1);
                    session = (Session) mconn.getConnection();
                }

                Iterator<String> classIter = classNames.iterator();
                ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
                while (classIter.hasNext())
                {
                    String className = classIter.next();
                    AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                    if (cmd != null && !cmd.isEmbeddedOnly() && cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE)
                    {
                        if (cmd instanceof ClassMetaData && ((ClassMetaData) cmd).isAbstract())
                        {
                            // No table required here
                            continue;
                        }

                        StoreData storeData = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                        Table table = null;
                        if (storeData != null)
                        {
                            table = storeData.getTable();
                        }
                        else
                        {
                            table = new CompleteClassTable(storeMgr, cmd, new SchemaVerifierImpl(storeMgr, cmd, clr));
                        }

                        // TODO Check existence of schema using
                        // "select keyspace_name from system.schema_keyspaces where keyspace_name='schema1';"
                        String schemaName = table.getSchemaName();
                        String tableName = table.getName();

                        NamingFactory namingFactory = storeMgr.getNamingFactory();
                        SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
                        boolean tableExists = checkTableExistence(session, stmtProvider, schemaName, tableName);
                        if (tableExists)
                        {
                            // Drop any class indexes
                            AbstractClassMetaData theCmd = cmd;
                            while (theCmd != null)
                            {
                                IndexMetaData[] clsIdxMds = theCmd.getIndexMetaData();
                                if (clsIdxMds != null)
                                {
                                    for (int i = 0; i < clsIdxMds.length; i++)
                                    {
                                        IndexMetaData idxmd = clsIdxMds[i];
                                        StringBuilder stmtBuilder = new StringBuilder("DROP INDEX ");
                                        String idxName = namingFactory.getConstraintName(theCmd, idxmd, i);

                                        if (ddlFileWriter == null)
                                        {
                                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.DropConstraint", stmtBuilder.toString()));
                                            session.execute(stmtBuilder.toString());
                                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.DropConstraint.Success", idxName));
                                        }
                                        else
                                        {
                                            try
                                            {
                                                ddlFileWriter.write(stmtBuilder.toString() + ";\n");
                                            }
                                            catch (IOException ioe)
                                            {
                                            }
                                        }
                                    }
                                }
                                theCmd = theCmd.getSuperAbstractClassMetaData();
                            }

                            // Drop any member-level indexes
                            int[] memberPositions = cmd.getAllMemberPositions();
                            for (int i = 0; i < memberPositions.length; i++)
                            {
                                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                                IndexMetaData idxmd = mmd.getIndexMetaData();
                                if (idxmd != null)
                                {
                                    StringBuilder stmtBuilder = new StringBuilder("DROP INDEX ");
                                    String idxName = namingFactory.getConstraintName(mmd, idxmd);

                                    if (ddlFileWriter == null)
                                    {
                                        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.DropConstraint", stmtBuilder.toString()));
                                        session.execute(stmtBuilder.toString());
                                        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.DropConstraint.Success", idxName));
                                    }
                                    else
                                    {
                                        try
                                        {
                                            ddlFileWriter.write(stmtBuilder.toString() + ";\n");
                                        }
                                        catch (IOException ioe)
                                        {
                                        }
                                    }
                                }
                            }

                            // Drop the table
                            StringBuilder stmtBuilder = new StringBuilder("DROP TABLE ");
                            if (schemaName != null)
                            {
                                stmtBuilder.append(schemaName).append('.');
                            }
                            stmtBuilder.append(tableName);

                            if (ddlFileWriter == null)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.DropTable", stmtBuilder.toString()));
                                session.execute(stmtBuilder.toString());
                                NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.DropTable.Success", tableName));
                            }
                            else
                            {
                                try
                                {
                                    ddlFileWriter.write(stmtBuilder.toString() + ";\n");
                                }
                                catch (IOException ioe)
                                {
                                }
                            }
                        }
                        else
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.DropTable.DoesntExist", tableName));
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

    public void validateSchema(Set<String> classNames, Properties props, Object connection)
    {
        Session session = (Session) connection;

        boolean success = true;
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        ManagedConnection mconn = null;
        try
        {
            if (session == null)
            {
                mconn = storeMgr.getConnection(-1);
                session = (Session) mconn.getConnection();
            }

            NamingFactory namingFactory = storeMgr.getNamingFactory();
            for (String className : classNames)
            {
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd.isEmbeddedOnly() || cmd.getPersistenceModifier() != ClassPersistenceModifier.PERSISTENCE_CAPABLE)
                {
                    continue;
                }
                if (cmd instanceof ClassMetaData && ((ClassMetaData) cmd).isAbstract())
                {
                    // No table required here
                    continue;
                }

                StoreData storeData = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                Table table = null;
                if (storeData != null)
                {
                    table = storeData.getTable();
                }
                else
                {
                    table = new CompleteClassTable(storeMgr, cmd, new SchemaVerifierImpl(storeMgr, cmd, clr));
                }

                // TODO Check existence of schema using
                // "select keyspace_name from system.schema_keyspaces where keyspace_name='schema1';"
                String schemaName = table.getSchemaName();
                String tableName = table.getName();

                SessionStatementProvider stmtProvider = ((CassandraStoreManager) storeMgr).getStatementProvider();
                boolean tableExists = checkTableExistence(session, stmtProvider, schemaName, tableName);
                if (!tableExists)
                {
                    NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.TableDoesntExist", cmd.getFullClassName(), tableName, schemaName));
                    success = false;
                }
                else
                {
                    // Check structure of the table against the required members
                    Map<String, ColumnDetails> tableStructure = getColumnDetailsForTable(session, stmtProvider, schemaName, tableName);
                    Set<String> colsFound = new HashSet();

                    List<Column> columns = table.getColumns();
                    for (Column column : columns)
                    {
                        ColumnDetails colDetails = getColumnDetailsForColumn(column, tableStructure);
                        if (colDetails == null)
                        {
                            // Column not present, so log it and fail the validation
                            if (column.getMemberColumnMapping() != null)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.ColumnForTableDoesntExist", tableName, column.getName(),
                                    column.getMemberColumnMapping().getMemberMetaData().getFullFieldName()));
                            }
                            else
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.ColumnForTableInvalidType", tableName, column.getName(),
                                    column.getColumnType()));
                            }
                            success = false;
                        }
                        else
                        {
                            String datastoreType = colDetails.typeName;
                            if (column.getTypeName().equals(datastoreType))
                            {
                                // Type is correct
                            }
                            else
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.ColumnTypeIncorrect", tableName, column.getName(),
                                    colDetails.typeName, column.getTypeName()));
                            }
                        }
                    }

                    if (success && tableStructure.size() != colsFound.size())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.ColumnCountIncorrect", tableName, colsFound.size(),
                            tableStructure.size()));
                        success = false;
                    }

                    // Check class-level indexes
                    AbstractClassMetaData theCmd = cmd;
                    while (theCmd != null)
                    {
                        IndexMetaData[] clsIdxMds = theCmd.getIndexMetaData();
                        if (clsIdxMds != null)
                        {
                            for (int i = 0; i < clsIdxMds.length; i++)
                            {
                                IndexMetaData idxmd = clsIdxMds[i];
                                String[] colNames = idxmd.getColumnNames();
                                if (colNames.length == 1)
                                {
                                    ColumnDetails colDetails = tableStructure.get(colNames[0].toLowerCase());
                                    String idxName = namingFactory.getConstraintName(theCmd, idxmd, i);
                                    if (colDetails == null || colDetails.indexName == null)
                                    {
                                        NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.TableIndexMissingForColumn", tableName,
                                            colNames[0]));
                                    }
                                    else
                                    {
                                        if (!idxName.equals(colDetails.indexName))
                                        {
                                            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName,
                                                colDetails.indexName));
                                        }
                                    }
                                }
                            }
                        }
                        theCmd = theCmd.getSuperAbstractClassMetaData();
                    }

                    // Add member-level indexes
                    int[] memberPositions = cmd.getAllMemberPositions();
                    for (int i = 0; i < memberPositions.length; i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(memberPositions[i]);
                        IndexMetaData idxmd = mmd.getIndexMetaData();
                        if (idxmd != null)
                        {
                            String colName = namingFactory.getColumnName(mmd, ColumnType.COLUMN);
                            ColumnDetails colDetails = tableStructure.get(colName.toLowerCase());
                            String idxName = namingFactory.getConstraintName(mmd, idxmd);
                            if (colDetails == null || colDetails.indexName == null)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.TableIndexMissingForColumn", tableName, colName));
                            }
                            else
                            {
                                if (!idxName.equals(colDetails.indexName))
                                {
                                    NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, colDetails.indexName));
                                }
                            }
                        }
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

        if (!success)
        {
            throw new NucleusException("Errors were encountered during validation of Cassandra schema");
        }
    }

    protected String createIndexCQL(String indexName, String schemaName, String tableName, String columnName, IndexMetaData idxmd)
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
        if (idxmd != null)
        {
            // Allow user-specifiable USING clause
            String using = idxmd.getValueForExtension("cassandra.createIndex.using");
            if (!StringUtils.isWhitespace(using))
            {
                stmtBuilder.append(" USING ").append(using);
            }
        }

        return stmtBuilder.toString();
    }

    public static boolean checkTableExistence(Session session, SessionStatementProvider stmtProvider, String schemaName, String tableName)
    {
        if (schemaName == null)
        {
            throw new NucleusUserException("Schema must be specified for table=" + tableName + " in order to check its existence");
        }
        StringBuilder stmtBuilder = new StringBuilder(
                "SELECT columnfamily_name FROM System.schema_columnfamilies WHERE keyspace_name=? AND columnfamily_name=?");
        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.CheckTableExistence", tableName, stmtBuilder.toString()));
        PreparedStatement stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
        ResultSet rs = session.execute(stmt.bind(schemaName.toLowerCase(), tableName.toLowerCase()));
        if (!rs.isExhausted())
        {
            return true;
        }
        return false;
    }

    public static boolean checkSchemaExistence(Session session, SessionStatementProvider stmtProvider, String schemaName)
    {
        if (schemaName == null)
        {
            throw new NucleusUserException("Schema must be specified in order to check its existence");
        }
        StringBuilder stmtBuilder = new StringBuilder("SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name=?;");
        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.CheckSchemaExistence", schemaName, stmtBuilder.toString()));
        PreparedStatement stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
        ResultSet rs = session.execute(stmt.bind(schemaName.toLowerCase()));
        if (!rs.isExhausted())
        {
            return true;
        }
        return false;
    }

    /**
     * Method to return the datastore structure for the specified table name in the specified schema. The
     * returned structure is a map of column details, keyed by the column name.
     * @param session The session
     * @param stmtProvider Provider for PreparedStatements
     * @param schemaName Name of the schema
     * @param tableName Name of the table
     * @return The table structure map
     */
    public Map<String, ColumnDetails> getColumnDetailsForTable(Session session, SessionStatementProvider stmtProvider, String schemaName, String tableName)
    {
        if (schemaName == null)
        {
            throw new NucleusUserException("Schema must be specified for table=" + tableName + " in order to check its structure");
        }
        StringBuilder stmtBuilder = new StringBuilder(
                "SELECT column_name, index_name, validator FROM system.schema_columns WHERE keyspace_name=? AND columnfamily_name=?");
        NucleusLogger.DATASTORE_SCHEMA.debug(Localiser.msg("Cassandra.Schema.CheckTableStructure", tableName, stmtBuilder.toString()));
        PreparedStatement stmt = stmtProvider.prepare(stmtBuilder.toString(), session);
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
            else if (validator.indexOf("IntegerType") >= 0 || validator.indexOf("Int32Type") >= 0)
            {
                typeName = "int";
            }
            else if (validator.indexOf("DoubleType") >= 0)
            {
                typeName = "double";
            }
            else if (validator.indexOf("DecimalType") >= 0)
            {
                typeName = "decimal";
            }
            else if (validator.indexOf("FloatType") >= 0)
            {
                typeName = "float";
            }
            else if (validator.indexOf("BooleanType") >= 0)
            {
                typeName = "boolean";
            }
            else if (validator.indexOf("DateType") >= 0)
            {
                typeName = "timestamp";
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

    public static class ColumnDetails
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

    private ColumnDetails getColumnDetailsForColumn(Column col, Map<String, ColumnDetails> tableStructure)
    {
        String colName = col.getName();
        if (colName.startsWith("\""))
        {
            // Remove any quotes if the identifier is quoted since table structure will not have quotes
            colName = colName.substring(1, colName.length() - 1);
        }
        return tableStructure.get(colName);
    }
}