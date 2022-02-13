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
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager for the persistence of an embedded PC object.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    /**
     * Constructor called when it is needed to null out all columns of an embedded object (and nested embedded
     * columns).
     * @param ec ExecutionContext
     * @param cmd Metadata for the class
     * @param insert Whether we are inserting
     * @param mmds Metadata for the member(s) leading to this embedded member
     * @param table The table we persist into
     */
    public StoreEmbeddedFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(ec, cmd, insert, table);
        this.mmds = mmds;
    }

    public StoreEmbeddedFieldManager(DNStateManager sm, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(sm, insert, table);
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
     * @see org.datanucleus.store.cassandra.fieldmanager.StoreFieldManager#storeObjectField(int,
     * java.lang.Object)
     */
    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        AbstractMemberMetaData lastMmd = mmds.get(mmds.size() - 1);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this member being a link back to the owner. TODO Repeat this for nested and
            // their owners
            if (sm != null)
            {
                DNStateManager[] ownerSMs = ec.getOwnersForEmbeddedStateManager(sm);
                if (ownerSMs != null && ownerSMs.length == 1 && value != ownerSMs[0].getObject())
                {
                    // Make sure the owner field is set
                    sm.replaceField(fieldNumber, ownerSMs[0].getObject());
                }
            }
            return;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, lastMmd))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                embMmds.add(mmd);

                if (value == null)
                {
                    // Store null in all columns of this and any nested embedded objects
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, insert, embMmds, table);
                    int[] embMmdPosns = embCmd.getAllMemberPositions();
                    for (int i = 0; i < embMmdPosns.length; i++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                        if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd
                                .getTypeName()))
                        {
                            // Store a null for any primitive/wrapper/String fields
                            List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                            colEmbMmds.add(embMmd);

                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colEmbMmds);
                            for (int j = 0; j < mapping.getNumberOfColumns(); j++)
                            {
                                columnValueByName.put(mapping.getColumn(j).getName(), null);
                            }
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                    Map<String, Object> embColValuesByName = storeEmbFM.getColumnValueByName();
                    columnValueByName.putAll(embColValuesByName);
                    return;
                }

                DNStateManager embSM = ec.findStateManagerForEmbedded(value, sm, mmd, null);
                StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(embSM, insert, embMmds, table);
                embSM.provideFields(embCmd.getAllMemberPositions(), storeEmbFM);
                Map<String, Object> embColValuesByName = storeEmbFM.getColumnValueByName();
                columnValueByName.putAll(embColValuesByName);
                return;
            }

            // TODO Embedded Collection
            NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded), storing as null");
            columnValueByName.put(getColumnMapping(fieldNumber).getColumn(0).getName(), null);
            return;
        }

        if (sm == null)
        {
            // Null the column
            MemberColumnMapping mapping = getColumnMapping(fieldNumber);
            for (int i = 0; i < mapping.getNumberOfColumns(); i++)
            {
                columnValueByName.put(mapping.getColumn(i).getName(), null);
            }
            return;
        }
        storeNonEmbeddedObjectField(mmd, relationType, clr, value);
    }
}