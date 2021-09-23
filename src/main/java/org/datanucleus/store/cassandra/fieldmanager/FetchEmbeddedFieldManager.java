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
package org.datanucleus.store.cassandra.fieldmanager;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * FieldManager for the retrieval of field values of an embedded PC object.
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public FetchEmbeddedFieldManager(ObjectProvider sm, Row row, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(sm, row, table);
        this.mmds = mmds;
    }

    public FetchEmbeddedFieldManager(ExecutionContext ec, Row row, AbstractClassMetaData cmd, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(ec, row, cmd, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.cassandra.fieldmanager.FetchFieldManager#fetchObjectField(int)
     */
    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this being a link back to the owner. TODO Repeat this for nested and their owners
            ObjectProvider[] ownerSms = ec.getOwnersForEmbeddedObjectProvider(op);
            return (ownerSms != null && ownerSms.length > 0 ? ownerSms[0].getObject() : null);
        }

        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                embMmds.add(mmd);
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                ObjectProvider embSM = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, embCmd, op, fieldNumber);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embSM, row, embMmds, table);
                embSM.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embSM.getObject();
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // TODO Embedded Collection
                NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded)");
            }
            return null; // Remove this when we support embedded
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }
}