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

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.converters.TypeConverter;

import com.datastax.driver.core.Row;

/**
 * FieldManager to use for retrieving values from Cassandra to put into a persistable object.
 */
public class FetchFieldManager extends AbstractFieldManager
{
    protected ObjectProvider op;

    protected Row row;

    /** Metadata of the owner field if this is for an embedded object. */
    protected AbstractMemberMetaData ownerMmd = null;

    public FetchFieldManager(ObjectProvider op, Row row)
    {
        this.op = op;
        this.row = row;
    }

    protected String getFieldName(int fieldNumber)
    {
        return op.getExecutionContext().getStoreManager().getNamingFactory().getColumnName(
            op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber), ColumnType.COLUMN);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchBooleanField(int)
     */
    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        return row.getBool(getFieldName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchCharField(int)
     */
    @Override
    public char fetchCharField(int fieldNumber)
    {
        return row.getString(getFieldName(fieldNumber)).charAt(0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchByteField(int)
     */
    @Override
    public byte fetchByteField(int fieldNumber)
    {
        return (byte) row.getInt(getFieldName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchShortField(int)
     */
    @Override
    public short fetchShortField(int fieldNumber)
    {
        return (short) row.getInt(getFieldName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchIntField(int)
     */
    @Override
    public int fetchIntField(int fieldNumber)
    {
        return row.getInt(getFieldName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchLongField(int)
     */
    @Override
    public long fetchLongField(int fieldNumber)
    {
        return row.getLong(getFieldName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchFloatField(int)
     */
    @Override
    public float fetchFloatField(int fieldNumber)
    {
        return row.getFloat(getFieldName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchDoubleField(int)
     */
    @Override
    public double fetchDoubleField(int fieldNumber)
    {
        return row.getDouble(getFieldName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchStringField(int)
     */
    @Override
    public String fetchStringField(int fieldNumber)
    {
        return row.getString(getFieldName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchObjectField(int)
     */
    @Override
    public Object fetchObjectField(int fieldNumber)
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
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            // TODO Get value for persistable object - trigger cascade persist
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // TODO Get value for collection/map of persistable objects - trigger cascade persist
        }
        else
        {
            TypeConverter stringConverter = op.getExecutionContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
            if (stringConverter != null)
            {
                return stringConverter.toMemberType(row.getString(getFieldName(fieldNumber)));
            }
            TypeConverter longConverter = op.getExecutionContext().getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
            if (longConverter != null)
            {
                return longConverter.toMemberType(row.getLong(getFieldName(fieldNumber)));
            }
        }

        // TODO Auto-generated method stub
        return super.fetchObjectField(fieldNumber);
    }
}