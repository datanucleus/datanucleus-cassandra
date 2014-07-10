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
package org.datanucleus.store.cassandra.query.expression;

import org.datanucleus.metadata.AbstractMemberMetaData;

/**
 * Expression for a field in a Cassandra table.
 */
public class CassandraFieldExpression extends CassandraExpression
{
    AbstractMemberMetaData mmd;

    String columnName;

    public CassandraFieldExpression(String columnName, AbstractMemberMetaData mmd)
    {
        this.columnName = columnName;
        this.mmd = mmd;
    }

    public AbstractMemberMetaData getMemberMetaData()
    {
        return mmd;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String toString()
    {
        return columnName;
    }
}