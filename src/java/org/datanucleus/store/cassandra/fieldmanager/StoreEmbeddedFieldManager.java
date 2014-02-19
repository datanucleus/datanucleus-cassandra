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
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager for the persistence of an embedded PC object.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public StoreEmbeddedFieldManager(ObjectProvider op, boolean insert, List<AbstractMemberMetaData> mmds)
    {
        super(op, insert);
        this.mmds = mmds;
    }

    protected String getColumnName(int fieldNumber)
    {
        // Find column name for embedded member
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        AbstractMemberMetaData[] embMmds = new AbstractMemberMetaData[mmds.size()+1];
        AbstractMemberMetaData[] inputMmds = mmds.toArray(new AbstractMemberMetaData[mmds.size()]);
        System.arraycopy(inputMmds, 0, embMmds, 0, inputMmds.length);
        embMmds[inputMmds.length] = mmd;
        return ec.getStoreManager().getNamingFactory().getColumnName(embMmds, 0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.cassandra.fieldmanager.StoreFieldManager#storeObjectField(int, java.lang.Object)
     */
    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        if (relationType != RelationType.NONE)
        {
            // TODO Likely need to update this method to pass in mmds in future?
            if (MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, mmds.get(mmds.size()-1)))
            {
                // Embedded field
                if (RelationType.isRelationSingleValued(relationType))
                {
                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                    embMmds.addAll(mmds);
                    embMmds.add(mmd);
                    AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(value.getClass(), clr);
                    ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(embOP, insert, embMmds);
                    embOP.provideFields(embCmd.getAllMemberPositions(), storeEmbFM);
                    Map<String, Object> embColValuesByName = storeEmbFM.getColumnValueByName();
                    columnValueByName.putAll(embColValuesByName);
                    return;
                }
                else
                {
                    // TODO Embedded Collection
                    NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded), storing as null");
                    columnValueByName.put(getColumnName(fieldNumber), null); // Remove this when we support embedded
                    objectValues.put(fieldNumber, null); // Remove this when we support embedded
                    return;
                }
            }
        }

        storeNonEmbeddedObjectField(mmd, relationType, clr, value);
    }
}