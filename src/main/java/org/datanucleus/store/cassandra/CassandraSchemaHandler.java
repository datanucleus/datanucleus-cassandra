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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
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
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

/**
 * Handler for schema management with Cassandra. Supports the following metadata extensions
 * <ul>
 * <li>ClassMetaData extension "cassandra.createTable.options" specifies any OPTIONS for a CREATE TABLE statement (comma-separated value if wanting multiple options)</li>
 * <li>IndexMetaData extension "cassandra.createIndex.using" specifies any USING clause for a CREATE INDEX statement</li>
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
     * Method to create a database (keyspace) in Cassandra. Accepts properties with names "replication", "durable_writes" (case sensitive).
     * @param catalogName Unused
     * @param schemaName Name of the schema
     * @param props Any properties defining the new keyspace
     */
    public void createDatabase(String catalogName, String schemaName, Properties props, Object connection)
    {
        CqlSession session = (CqlSession) connection;
        ManagedConnection mconn = null;
        try
        {
            if (session == null)
            {
                mconn = storeMgr.getConnectionManager().getConnection(-1);
                session = (CqlSession) mconn.getConnection();
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
        CqlSession session = (CqlSession) connection;
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
                    mconn = storeMgr.getConnectionManager().getConnection(-1);
                    session = (CqlSession) mconn.getConnection();
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
    protected void createSchemaForClass(AbstractClassMetaData cmd, CqlSession session, ClassLoaderResolver clr, List<String> tableStmts, List<String> constraintStmts)
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

        String schemaName = table.getSchemaName();
        if (schemaName != null)
        {
            // Allow auto creation of schema
            Optional<KeyspaceMetadata> schemaKeyspace = session.getMetadata().getKeyspace(schemaName);
            if (schemaKeyspace.isEmpty())
            {
                if (autoCreateDatabase)
                {
                    // TODO Localise this
                    NucleusLogger.GENERAL.info("Creating DB since schema " + schemaName + " doesnt exist");
                    createDatabase(null, schemaName, null, session);
                }
            }
        }

        TableMetadata tmd = getTableMetadata(session, schemaName, table.getName());
        if (tmd != null)
        {
            // Add/delete any columns/constraints to match the current definition (aka "schema evolution") TODO Use tmd
            if (autoCreateColumns)
            {
                // Add any missing columns
                List<Column> columns = table.getColumns();
                for (Column column : columns)
                {
                    ColumnMetadata colmd = getColumnMetadataForColumn(tmd, column);
                    if (colmd == null)
                    {
                        // Add column since doesn't exist
                        StringBuilder stmtBuilder = new StringBuilder("ALTER TABLE ");
                        if (schemaName != null)
                        {
                            stmtBuilder.append(schemaName).append('.');
                        }
                        stmtBuilder.append(table.getName());
                        stmtBuilder.append(" ADD ");
                        stmtBuilder.append(column.getName()).append(" ").append(column.getTypeName());
                        tableStmts.add(stmtBuilder.toString());
                    }
                    else
                    {
                        String colTypeNameDB = getTypeNameForColumn(colmd);
                        if (colTypeNameDB != null && !colTypeNameDB.equalsIgnoreCase(column.getTypeName())) // TODO Is it case insensitive?
                        {
                            // TODO Change the column type if requested. What about existing data
                            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.TableColumnTypeIncorrect", table.getName(), column.getName(),
                                colTypeNameDB, column.getTypeName()));
                        }
                    }
                }
            }

            if (autoDeleteColumns)
            {
                // Delete any columns that are not needed by this class
                Map<CqlIdentifier, ColumnMetadata> colmdById = tmd.getColumns();
                Collection<ColumnMetadata> colmds = colmdById.values();
                for (ColumnMetadata colmd : colmds)
                {
                    CqlIdentifier colNameId = colmd.getName();
                    String colName = colNameId.toString();
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
                        stmtBuilder.append(" DROP ");
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
                    List<IndexMetaData> clsIdxMds = theCmd.getIndexMetaData();
                    if (clsIdxMds != null)
                    {
                        int i = 0;
                        for (IndexMetaData idxmd : clsIdxMds)
                        {
                            String[] colNames = idxmd.getColumnNames();
                            if (colNames.length > 1)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexForClassWithMultipleColumns", theCmd.getFullClassName()));
                            }
                            else
                            {
                                String idxName = namingFactory.getConstraintName(theCmd, idxmd, i++);
                                ColumnMetadata dbColMd = getColumnMetadataForColumnName(tmd, colNames[0]);
                                if (dbColMd == null)
                                {
                                    // Column will have been added above, so add the index
                                    String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), colNames[0], idxmd);
                                    constraintStmts.add(indexStmt);
                                }
                                else
                                {
                                    IndexMetadata dbIdxMdForCol = getIndexMetadataForColumn(tmd, colNames[0]);
                                    if (dbIdxMdForCol == null)
                                    {
                                        // Index doesn't yet exist
                                        String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), colNames[0], idxmd);
                                        constraintStmts.add(indexStmt);
                                    }
                                    else if (!idxName.equals(dbIdxMdForCol.getName()))
                                    {
                                        // Index has wrong name!
                                        NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, dbIdxMdForCol.getName()));
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
                        Column[] cols = mapping.getColumns();
                        if (cols != null)
                        {
                            if (cols.length > 1)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexForMemberWithMultipleColumns", mapping.getMemberMetaData().getFullFieldName()));
                            }
                            else if (cols.length == 1)
                            {
                                Column column = cols[0];
                                ColumnMetadata dbColMd = getColumnMetadataForColumn(tmd, column);
                                String idxName = namingFactory.getConstraintName(cmd.getName(), mapping.getMemberMetaData(), idxmd);
                                if (dbColMd == null)
                                {
                                    // Column will have been added above, so add the index
                                    String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), idxmd);
                                    constraintStmts.add(indexStmt);
                                }
                                else
                                {
                                    IndexMetadata dbIdxMd = getIndexMetadataForColumn(tmd, column.getName());
                                    if (dbIdxMd == null)
                                    {
                                        // Index doesn't yet exist
                                        String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), idxmd);
                                        constraintStmts.add(indexStmt);
                                    }
                                    else if (!idxName.equalsIgnoreCase(dbIdxMd.getName().toString()))
                                    {
                                        // Index has wrong name!
                                        NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, dbIdxMd.getName()));
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
                        Column column = table.getSurrogateColumn(SurrogateColumnType.VERSION);
                        ColumnMetadata dbVerColMd = getColumnMetadataForColumn(tmd, column);
                        String idxName = namingFactory.getConstraintName(cmd, vermd.getIndexMetaData(), ColumnType.VERSION_COLUMN);
                        if (dbVerColMd == null)
                        {
                            String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), vermd.getIndexMetaData());
                            constraintStmts.add(indexStmt);
                        }
                        else
                        {
                            IndexMetadata dbVerIdxMd = getIndexMetadataForColumn(tmd, column.getName());
                            if (dbVerIdxMd == null)
                            {
                                // Index doesn't yet exist
                                String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), vermd.getIndexMetaData());
                                constraintStmts.add(indexStmt);
                            }
                            else if (!idxName.equals(dbVerIdxMd.getName()))
                            {
                                // Index has wrong name!<?xml version="1.0"?>

                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, dbVerIdxMd.getName()));
                            }
                        }
                    }
                }

                if (cmd.hasDiscriminatorStrategy())
                {
                    DiscriminatorMetaData dismd = cmd.getDiscriminatorMetaData();
                    if (dismd.getIndexMetaData() != null)
                    {
                        Column column = table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR);
                        ColumnMetadata dbDiscColMd = getColumnMetadataForColumn(tmd, column);
                        String idxName = namingFactory.getConstraintName(cmd, dismd.getIndexMetaData(), ColumnType.DISCRIMINATOR_COLUMN);
                        if (dbDiscColMd == null)
                        {
                            String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), dismd.getIndexMetaData());
                            constraintStmts.add(indexStmt);
                        }
                        else
                        {
                            IndexMetadata dbDiscIdxMd = getIndexMetadataForColumn(tmd, column.getName());
                            if (dbDiscIdxMd == null)
                            {
                                // Index doesn't yet exist
                                String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), dismd.getIndexMetaData());
                                constraintStmts.add(indexStmt);
                            }
                            else if (!idxName.equals(dbDiscIdxMd.getName()))
                            {
                                // Index has wrong name!
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, dbDiscIdxMd.getName()));
                            }
                        }
                    }
                }
                if (storeMgr.getNucleusContext().isClassMultiTenant(cmd))
                {
                    // Index the multitenancy column
                    Column column = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY);
                    String idxName = namingFactory.getConstraintName(cmd, null, ColumnType.MULTITENANCY_COLUMN);
                    ColumnMetadata dbMultiColMd = getColumnMetadataForColumn(tmd, column);
                    if (dbMultiColMd == null)
                    {
                        // Doesnt exist in Cassandra
                        //String idxName = cmd.getName() + "_TENANCY_IDX";
                        String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), null);
                        constraintStmts.add(indexStmt);
                    }
                    else
                    {
                        // Exists in Cassandra
                        IndexMetadata dbMultiIdxMd = getIndexMetadataForColumn(tmd, column.getName());
                        if (dbMultiIdxMd == null)
                        {
                            // Index doesn't yet exist
                            String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), null);
                            constraintStmts.add(indexStmt);
                        }
                        else if (!idxName.equals(dbMultiIdxMd.getName()))
                        {
                            // Index has wrong name!
                            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, dbMultiIdxMd.getName()));
                        }
                    }
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
                    List<IndexMetaData> clsIdxMds = theCmd.getIndexMetaData();
                    if (clsIdxMds != null)
                    {
                        int i = 0;
                        for (IndexMetaData idxmd : clsIdxMds)
                        {
                            String[] colNames = idxmd.getColumnNames();
                            if (colNames.length > 1)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexForClassWithMultipleColumns",
                                    theCmd.getFullClassName()));
                            }
                            else
                            {
                                String idxName = namingFactory.getConstraintName(theCmd, idxmd, i++);
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
                            String idxName = namingFactory.getConstraintName(cmd.getName(), mmd, idxmd);
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
                        Column column = table.getSurrogateColumn(SurrogateColumnType.VERSION);
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
                        Column column = table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR);
                        String idxName = namingFactory.getConstraintName(cmd, dismd.getIndexMetaData(), ColumnType.DISCRIMINATOR_COLUMN);
                        String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), dismd.getIndexMetaData());
                        constraintStmts.add(indexStmt);
                    }
                }
                if (storeMgr.getNucleusContext().isClassMultiTenant(cmd))
                {
                    Column column = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY);
                    String idxName = namingFactory.getConstraintName(cmd, null, ColumnType.MULTITENANCY_COLUMN);
                    String indexStmt = createIndexCQL(idxName, schemaName, table.getName(), column.getName(), null);
                    constraintStmts.add(indexStmt);
                }
            }
        }
    }

    /**
     * Method to drop a database (keyspace) in Cassandra.
     * @param catalogName Unused
     * @param schemaName Name of the schema (keyspace).
     * @param props Any properties controlling deletion
     * @param connection Connection to use (null implies this will obtain its own connection)
     */
    public void deleteDatabase(String catalogName, String schemaName, Properties props, Object connection)
    {
        CqlSession session = (CqlSession) connection;
        ManagedConnection mconn = null;
        try
        {
            if (session == null)
            {
                mconn = storeMgr.getConnectionManager().getConnection(-1);
                session = (CqlSession) mconn.getConnection();
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
        CqlSession session = (CqlSession) connection;
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
                    mconn = storeMgr.getConnectionManager().getConnection(-1);
                    session = (CqlSession) mconn.getConnection();
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
                        TableMetadata tmd = getTableMetadata(session, schemaName, tableName);
                        if (tmd != null)
                        {
                            // Drop any class indexes
                            AbstractClassMetaData theCmd = cmd;
                            while (theCmd != null)
                            {
                                List<IndexMetaData> clsIdxMds = theCmd.getIndexMetaData();
                                if (clsIdxMds != null)
                                {
                                    int i = 0;
                                    for (IndexMetaData idxmd : clsIdxMds)
                                    {
                                        StringBuilder stmtBuilder = new StringBuilder("DROP INDEX ");
                                        String idxName = namingFactory.getConstraintName(theCmd, idxmd, i++);

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
                                    String idxName = namingFactory.getConstraintName(null, mmd, idxmd);

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
        CqlSession session = (CqlSession) connection;

        boolean success = true;
        ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
        ManagedConnection mconn = null;
        try
        {
            if (session == null)
            {
                mconn = storeMgr.getConnectionManager().getConnection(-1);
                session = (CqlSession) mconn.getConnection();
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

                TableMetadata tmd = getTableMetadata(session, schemaName, tableName);
                if (tmd == null)
                {
                    NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.TableDoesntExist", cmd.getFullClassName(), tableName, schemaName));
                    success = false;
                }
                else
                {
                    // Check structure of the table against the required members TODO Use tmd
                    Set<String> colsFound = new HashSet();

                    List<Column> columns = table.getColumns();
                    for (Column column : columns)
                    {
                        ColumnMetadata colmd = getColumnMetadataForColumn(tmd, column);
                        if (colmd == null)
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
                            String datastoreType = getTypeNameForColumn(colmd);
                            if (column.getTypeName().equals(datastoreType))
                            {
                                // Type is correct
                            }
                            else
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.ColumnTypeIncorrect", tableName, column.getName(),
                                    datastoreType, column.getTypeName()));
                            }
                        }
                    }

                    int numColsDB = tmd.getColumns().size();
                    if (success && numColsDB != colsFound.size())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.ColumnCountIncorrect", tableName, colsFound.size(), numColsDB));
                        success = false;
                    }

                    // Check class-level indexes
                    AbstractClassMetaData theCmd = cmd;
                    while (theCmd != null)
                    {
                        List<IndexMetaData> clsIdxMds = theCmd.getIndexMetaData();
                        if (clsIdxMds != null)
                        {
                            int i = 0;
                            for (IndexMetaData idxmd : clsIdxMds)
                            {
                                String[] colNames = idxmd.getColumnNames();
                                if (colNames.length == 1)
                                {
                                    ColumnMetadata dbColMd = getColumnMetadataForColumnName(tmd, colNames[0].toLowerCase());
                                    IndexMetadata dbIdxMd = getIndexMetadataForColumn(tmd, colNames[0].toLowerCase());
                                    String idxName = namingFactory.getConstraintName(theCmd, idxmd, i++);
                                    if (dbColMd == null || dbIdxMd == null)
                                    {
                                        NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.TableIndexMissingForColumn", tableName, colNames[0]));
                                    }
                                    else
                                    {
                                        if (!idxName.equals(dbIdxMd.getName()))
                                        {
                                            NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, dbIdxMd.getName()));
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
                            ColumnMetadata dbColMd = getColumnMetadataForColumnName(tmd, colName.toLowerCase());
                            IndexMetadata dbIdxMd = getIndexMetadataForColumn(tmd, colName.toLowerCase());
                            String idxName = namingFactory.getConstraintName(null, mmd, idxmd);
                            if (dbColMd == null || dbIdxMd == null)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.error(Localiser.msg("Cassandra.Schema.TableIndexMissingForColumn", tableName, colName));
                            }
                            else
                            {
                                if (!idxName.equals(dbIdxMd.getName()))
                                {
                                    NucleusLogger.DATASTORE_SCHEMA.warn(Localiser.msg("Cassandra.Schema.IndexHasWrongName", idxName, dbIdxMd.getName()));
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

    public static TableMetadata getTableMetadata(CqlSession session, String schemaName, String tableName)
    {
        KeyspaceMetadata ks = getKeyspaceMetadata(session, schemaName);
        if (ks == null)
        {
            return null;
        }
        Optional<TableMetadata> tabmd = ks.getTable(tableName.toLowerCase());
        return tabmd.isPresent() ? tabmd.get() : null;
    }

    public static KeyspaceMetadata getKeyspaceMetadata(CqlSession session, String schemaName)
    {
        if (schemaName == null)
        {
            throw new NucleusUserException("Schema name must be specified in order to check its existence");
        }
        Optional<KeyspaceMetadata> ksmd = session.getMetadata().getKeyspace(schemaName.toLowerCase());
        return ksmd.isPresent() ? ksmd.get() : null;
    }

    protected String getTypeNameForColumn(ColumnMetadata colmd)
    {
        String typeName = colmd.getType().toString();
        typeName = StringUtils.replaceAll(typeName, "text", "varchar"); // Put all "text" as "varchar" since using that internally
        typeName = StringUtils.replaceAll(typeName, ", ", ","); // Omit space from any multi-dimension specifications e.g "map<type1, type2>"
        return typeName;
    }

    protected ColumnMetadata getColumnMetadataForColumn(TableMetadata tmd, Column col)
    {
        String colName = col.getName();
        return getColumnMetadataForColumnName(tmd, colName);
    }

    protected ColumnMetadata getColumnMetadataForColumnName(TableMetadata tmd, String colName)
    {
        if (colName.startsWith("\""))
        {
            // Remove any quotes if the identifier is quoted since table structure will not have quotes
            colName = colName.substring(1, colName.length() - 1);
        }
        Optional<ColumnMetadata> colmd = tmd.getColumn(colName);
        return colmd.isPresent() ? colmd.get() : null;
    }

    protected IndexMetadata getIndexMetadataForColumn(TableMetadata tmd, String colName)
    {
        Map<CqlIdentifier, IndexMetadata> idxmdsById = tmd.getIndexes();
        Collection<IndexMetadata> idxmds = idxmdsById.values();
        if (idxmds != null)
        {
            for (IndexMetadata idxmd : idxmds)
            {
                String dbIdxTarget = idxmd.getTarget();
                if (dbIdxTarget != null && dbIdxTarget.equals(colName))
                {
                    return idxmd;
                }
            }
        }
        return null;
    }
}