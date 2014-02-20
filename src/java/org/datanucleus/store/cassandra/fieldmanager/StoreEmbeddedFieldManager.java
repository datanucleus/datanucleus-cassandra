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
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * FieldManager for the persistence of an embedded PC object.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    /**
     * Constructor called when it is needed to null out all columns of an embedded object (and nsted embedded columns).
     */
    public StoreEmbeddedFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, boolean insert, List<AbstractMemberMetaData> mmds)
    {
        super(ec, cmd, insert);
        this.mmds = mmds;
    }

    public StoreEmbeddedFieldManager(ObjectProvider op, boolean insert, List<AbstractMemberMetaData> mmds)
    {
        super(op, insert);
        this.mmds = mmds;
    }

    protected String getColumnName(int fieldNumber)
    {
        // Find column name for embedded member
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        NucleusLogger.GENERAL.info(">> StoreEmbFM.getCol embMmds=" + StringUtils.collectionToString(embMmds) + " num=" + embMmds.size() + " col=" + ec.getStoreManager().getNamingFactory().getColumnName(embMmds, 0));
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
            if (MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, mmds.get(mmds.size()-1)))
            {
                // Embedded field
                if (RelationType.isRelationSingleValued(relationType))
                {
                    AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    int[] embMmdPosns = embCmd.getAllMemberPositions();
                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                    embMmds.add(mmd);
                    if (value == null)
                    {
                        StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, insert, embMmds);
                        for (int i=0;i<embMmdPosns.length;i++)
                        {
                            AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                            if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd.getTypeName()))
                            {
                                // Store a null for any primitive/wrapper/String fields
                                List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                                colEmbMmds.add(embMmd);
                                String colName = ec.getStoreManager().getNamingFactory().getColumnName(colEmbMmds, 0);
                                columnValueByName.put(colName, null);
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
                    columnValueByName.put(getColumnName(fieldNumber), null);
                    return;
                }
            }
        }

        if (op == null)
        {
            // Null the column
            columnValueByName.put(getColumnName(fieldNumber), null);
            return;
        }
        storeNonEmbeddedObjectField(mmd, relationType, clr, value);
    }
}