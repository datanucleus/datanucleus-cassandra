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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager for the storing of field values into Cassandra.
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    Map<Integer, Object> objectValues = new HashMap<Integer, Object>();

    /** Metadata of the owner field if this is for an embedded object. */
    protected AbstractMemberMetaData ownerMmd = null;

    public StoreFieldManager(ObjectProvider op, boolean insert)
    {
        super(op, insert);
    }

    public Object[] getValuesToStore()
    {
        // Make sure we return them in field order
        Object[] values = new Object[objectValues.size()];
        int i = 0;
        for (Integer fieldNum : objectValues.keySet())
        {
            values[i++] = objectValues.get(fieldNum);
        }
        return values;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeBooleanField(int, boolean)
     */
    @Override
    public void storeBooleanField(int fieldNumber, boolean value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeCharField(int, char)
     */
    @Override
    public void storeCharField(int fieldNumber, char value)
    {
        objectValues.put(fieldNumber, "" + value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeByteField(int, byte)
     */
    @Override
    public void storeByteField(int fieldNumber, byte value)
    {
        objectValues.put(fieldNumber, (int)value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeShortField(int, short)
     */
    @Override
    public void storeShortField(int fieldNumber, short value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeIntField(int, int)
     */
    @Override
    public void storeIntField(int fieldNumber, int value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeLongField(int, long)
     */
    @Override
    public void storeLongField(int fieldNumber, long value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeFloatField(int, float)
     */
    @Override
    public void storeFloatField(int fieldNumber, float value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeDoubleField(int, double)
     */
    @Override
    public void storeDoubleField(int fieldNumber, double value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeStringField(int, java.lang.String)
     */
    @Override
    public void storeStringField(int fieldNumber, String value)
    {
        objectValues.put(fieldNumber, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeObjectField(int, java.lang.Object)
     */
    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(op.getExecutionContext().getClassLoaderResolver());

        boolean embedded = false;
        if (relationType != RelationType.NONE)
        {
            // Determine if this relation field is embedded
            if (RelationType.isRelationSingleValued(relationType))
            {
                if (ownerMmd != null && ownerMmd.getEmbeddedMetaData() != null)
                {
                    // Is this a nested embedded (from JDO definition) with specification for this field?
                    AbstractMemberMetaData[] embMmds = ownerMmd.getEmbeddedMetaData().getMemberMetaData();
                    if (embMmds != null)
                    {
                        for (int i=0;i<embMmds.length;i++)
                        {
                            if (embMmds[i].getName().equals(mmd.getName()))
                            {
                                embedded = true;
                            }
                        }
                    }
                }
            }

            if (mmd.isEmbedded() || mmd.getEmbeddedMetaData() != null)
            {
                // Does this field have @Embedded definition?
                embedded = true;
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // Is this an embedded element/key/value?
                if (mmd.hasCollection() && mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null)
                {
                    // Embedded collection element
                    embedded = true;
                }
                else if (mmd.hasArray() && mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null)
                {
                    // Embedded array element
                    embedded = true;
                }
                else if (mmd.hasMap() && 
                        ((mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getEmbeddedMetaData() != null) || 
                        (mmd.getValueMetaData() != null && mmd.getValueMetaData().getEmbeddedMetaData() != null)))
                {
                    // Embedded map key/value
                    embedded = true;
                }
            }
        }

        if (embedded)
        {
            if (RelationType.isRelationSingleValued(relationType))
            {
                // TODO Embedded PC object
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // TODO Embedded Collection
            }
            return;
        }

        if (value == null)
        {
            objectValues.put(fieldNumber, null);
            return;
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            Object valuePC = op.getExecutionContext().persistObjectInternal(value, op, fieldNumber, -1);
            Object valueID = op.getExecutionContext().getApiAdapter().getIdForObject(valuePC);
            objectValues.put(fieldNumber, valueID.toString());
            return;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            if (mmd.hasCollection())
            {
                Collection<String> idColl = null;
                if (value instanceof List)
                {
                    idColl = new ArrayList<String>();
                }
                else
                {
                    idColl = new HashSet<String>();
                }
                Collection coll = (Collection)value;
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    Object elementPC = op.getExecutionContext().persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = op.getExecutionContext().getApiAdapter().getIdForObject(elementPC);
                    idColl.add(elementID.toString());
                }
                objectValues.put(fieldNumber, idColl);
                return;
            }
            else if (mmd.hasMap())
            {
                // TODO Support maps
            }
            else if (mmd.hasArray())
            {
                // TODO Support arrays
            }
            // TODO Get value for collection/map of persistable objects - trigger cascade persist
        }
        else
        {
            if (value instanceof Enum)
            {
                // Persist as ordinal unless user specifies jdbc-type of "varchar"
                ColumnMetaData[] colmds = mmd.getColumnMetaData();
                if (colmds != null && colmds.length == 1)
                {
                    if (colmds[0].getJdbcType().equalsIgnoreCase("varchar"))
                    {
                        objectValues.put(fieldNumber, ((Enum)value).name());
                        return;
                    }
                }
                objectValues.put(fieldNumber, ((Enum)value).ordinal());
                return;
            }
            if (value instanceof Date)
            {
                objectValues.put(fieldNumber, value);
                return;
            }

            TypeConverter stringConverter = op.getExecutionContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
            if (stringConverter != null)
            {
                objectValues.put(fieldNumber, stringConverter.toDatastoreType(value));
                return;
            }
            TypeConverter longConverter = op.getExecutionContext().getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
            if (longConverter != null)
            {
                objectValues.put(fieldNumber, longConverter.toDatastoreType(value));
                return;
            }
        }

        NucleusLogger.PERSISTENCE.warn("Not generated persistable value for field " + mmd.getFullFieldName() + " so putting null");
        objectValues.put(fieldNumber, null);
    }
}