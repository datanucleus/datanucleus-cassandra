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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.ColumnAttributer;
import org.datanucleus.util.NucleusLogger;

/**
 * Attributer for columns for Cassandra stores.
 * Used in the process of generating Table objects.
 */
public class ColumnAttributerImpl implements ColumnAttributer
{
    StoreManager storeMgr;
    AbstractClassMetaData cmd;
    ClassLoaderResolver clr;

    public ColumnAttributerImpl(StoreManager storeMgr, AbstractClassMetaData cmd, ClassLoaderResolver clr)
    {
        this.storeMgr = storeMgr;
        this.cmd = cmd;
        this.clr = clr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.table.ColumnAttributer#attributeColumn(org.datanucleus.store.schema.table.Column, org.datanucleus.metadata.AbstractMemberMetaData[])
     */
    @Override
    public void attributeColumn(Column col, AbstractMemberMetaData... mmds)
    {
        if (mmds != null && mmds.length > 0)
        {
            AbstractMemberMetaData mmd = mmds[mmds.length-1];
            NucleusLogger.GENERAL.info(">> col=" + col + " mmd=" + mmd);
            String cassandraType = CassandraUtils.getCassandraColumnTypeForMember(mmd, storeMgr.getNucleusContext().getTypeManager(), clr);
            if (cassandraType == null)
            {
                NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmd.getFullFieldName() + " of type "+ mmd.getTypeName() + " has no supported cassandra type! Ignoring");
            }
            else
            {
                col.setTypeName(cassandraType);
                // TODO Assign TypeConverter
            }
        }
        else
        {
            if (col.getColumnType() == ColumnType.DATASTOREID_COLUMN)
            {
                col.setTypeName("bigint"); // TODO Set the type based on jdbc-type of the datastore-id metadata : uuid?, varchar?
                // TODO Assign TypeConverter
            }
            else if (col.getColumnType() == ColumnType.VERSION_COLUMN)
            {
                String cassandraType = cmd.getVersionMetaDataForClass().getVersionStrategy() == VersionStrategy.DATE_TIME ? "timestamp" : "int";
                col.setTypeName(cassandraType);
                // TODO Assign TypeConverter
            }
            else if (col.getColumnType() == ColumnType.DISCRIMINATOR_COLUMN)
            {
                col.setTypeName("varchar");
                // TODO Assign TypeConverter
            }
            else if (col.getColumnType() == ColumnType.MULTITENANCY_COLUMN)
            {
                col.setTypeName("varchar");
                // TODO Assign TypeConverter
            }
        }
    }
}