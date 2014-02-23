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
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.Row;

/**
 * FieldManager for the retrieval of field values of an embedded PC object.
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public FetchEmbeddedFieldManager(ObjectProvider op, Row row, List<AbstractMemberMetaData> mmds)
    {
        super(op, row);
        this.mmds = mmds;
    }

    public FetchEmbeddedFieldManager(ExecutionContext ec, Row row, AbstractClassMetaData cmd, List<AbstractMemberMetaData> mmds)
    {
        super(ec, row, cmd);
        this.mmds = mmds;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.cassandra.fieldmanager.FetchFieldManager#getColumnName(int)
     */
    @Override
    protected String getColumnName(int fieldNumber)
    {
        // Find column name for embedded member
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return ec.getStoreManager().getNamingFactory().getColumnName(embMmds, 0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.cassandra.fieldmanager.FetchFieldManager#fetchObjectField(int)
     */
    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);

        if (relationType != RelationType.NONE)
        {
            EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
            if (MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
            {
                // Embedded field
                if (RelationType.isRelationSingleValued(relationType))
                {
                    if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
                    {
                        // Special case of this being a link back to the owner. TODO Repeat this for nested and their owners
                        return op.getObject();
                    }

                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                    embMmds.add(mmd);
                    AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    ObjectProvider embOP = ec.newObjectProviderForEmbedded(embCmd, op, fieldNumber);
                    FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, row, embMmds);
                    embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                    return embOP.getObject();
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    // TODO Embedded Collection
                    NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded)");
                }
                return null; // Remove this when we support embedded
            }
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }
}