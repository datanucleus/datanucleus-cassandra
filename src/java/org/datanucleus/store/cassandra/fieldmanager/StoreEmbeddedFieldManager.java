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

import java.util.List;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.ObjectProvider;

/**
 * FieldManager for the persistence of an embedded PC object.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds; // TODO Is this needed, for example, to get column name?

    public StoreEmbeddedFieldManager(ObjectProvider op, boolean insert, List<AbstractMemberMetaData> mmds)
    {
        super(op, insert);
        this.mmds = mmds;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.cassandra.fieldmanager.StoreFieldManager#storeObjectField(int, java.lang.Object)
     */
    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        // TODO Implement this, and also allow for nested embedded members
        super.storeObjectField(fieldNumber, value);
    }
}