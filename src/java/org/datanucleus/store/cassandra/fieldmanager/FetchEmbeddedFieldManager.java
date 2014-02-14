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

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.ObjectProvider;

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
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        AbstractMemberMetaData[] embMmds = new AbstractMemberMetaData[mmds.size()+1];
        AbstractMemberMetaData[] inputMmds = mmds.toArray(new AbstractMemberMetaData[mmds.size()]);
        System.arraycopy(inputMmds, 0, embMmds, 0, inputMmds.length);
        embMmds[inputMmds.length] = mmd;
        return ec.getStoreManager().getNamingFactory().getColumnName(embMmds, 0);
    }
}