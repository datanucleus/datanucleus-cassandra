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

import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.CassandraUtils.CassandraTypeDetails;
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
     * @see org.datanucleus.store.schema.table.ColumnAttributer#attributeColumn(org.datanucleus.store.schema.table.Column, org.datanucleus.metadata.AbstractMemberMetaData)
     */
    @Override
    public void attributeColumn(Column col, AbstractMemberMetaData mmd)
    {
        if (mmd != null)
        {
            CassandraTypeDetails typeDetails = CassandraUtils.getCassandraColumnTypeForMember(mmd, storeMgr.getNucleusContext().getTypeManager(), clr);
            String cassandraType = typeDetails.typeName;
            if (cassandraType == null)
            {
                NucleusLogger.DATASTORE_SCHEMA.warn("Member " + mmd.getFullFieldName() + " of type "+ mmd.getTypeName() + " has no supported cassandra type! Ignoring");
            }
            else
            {
                col.setTypeName(cassandraType);
                if (typeDetails.typeConverter != null)
                {
                    col.setTypeConverter(typeDetails.typeConverter);
                }
            }
        }
        else
        {
            if (col.getColumnType() == ColumnType.DATASTOREID_COLUMN)
            {
                String type = "bigint"; // Default to bigint unless specified
                IdentityMetaData idmd = cmd.getIdentityMetaData();
                if (idmd != null && idmd.getColumnMetaData() != null && idmd.getColumnMetaData().getJdbcType() != null)
                {
                    type = idmd.getColumnMetaData().getJdbcType();
                }
                // TODO Set based on value generator
                col.setTypeName(type);
            }
            else if (col.getColumnType() == ColumnType.VERSION_COLUMN)
            {
                String cassandraType = cmd.getVersionMetaDataForClass().getVersionStrategy() == VersionStrategy.DATE_TIME ? "timestamp" : "bigint";
                col.setTypeName(cassandraType);
            }
            else if (col.getColumnType() == ColumnType.DISCRIMINATOR_COLUMN)
            {
                col.setTypeName("varchar");
            }
            else if (col.getColumnType() == ColumnType.MULTITENANCY_COLUMN)
            {
                col.setTypeName("varchar");
            }
        }
    }

    @Override
    public void attributeEmbeddedColumn(Column col, List<AbstractMemberMetaData> mmds)
    {
        AbstractMemberMetaData mmd = mmds.get(mmds.size()-1);
        CassandraTypeDetails typeDetails = CassandraUtils.getCassandraColumnTypeForMember(mmd, storeMgr.getNucleusContext().getTypeManager(), clr);
        String cassandraType = typeDetails.typeName;
        if (cassandraType == null)
        {
            NucleusLogger.DATASTORE_SCHEMA.warn("Embedded member " + mmd.getFullFieldName() + " of type "+ mmd.getTypeName() + " has no supported cassandra type! Ignoring");
        }
        else
        {
            col.setTypeName(cassandraType);
            if (typeDetails.typeConverter != null)
            {
                col.setTypeConverter(typeDetails.typeConverter);
            }
        }
    }
}