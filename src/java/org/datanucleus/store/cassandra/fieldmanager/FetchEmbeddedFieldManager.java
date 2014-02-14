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

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.state.ObjectProvider;

import com.datastax.driver.core.Row;

/**
 * FieldManager for the retrieval of field values of an embedded PC object.
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    public FetchEmbeddedFieldManager(ObjectProvider op, Row row)
    {
        super(op, row);
    }

    public FetchEmbeddedFieldManager(ExecutionContext ec, Row row, AbstractClassMetaData cmd)
    {
        super(ec, row, cmd);
        // TODO Auto-generated constructor stub
    }

}