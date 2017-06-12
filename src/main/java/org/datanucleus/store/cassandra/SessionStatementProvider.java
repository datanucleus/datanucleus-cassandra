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
package org.datanucleus.store.cassandra;

import java.util.Map;

import org.datanucleus.util.ConcurrentReferenceHashMap;
import org.datanucleus.util.ConcurrentReferenceHashMap.ReferenceType;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * Provider for PreparedStatements for a Session. Provides a cache of PreparedStatement for the Session.
 */
public class SessionStatementProvider
{
    Map<String, PreparedStatement> preparedStatementCache = new ConcurrentReferenceHashMap<>(1, ReferenceType.STRONG, ReferenceType.SOFT);

    public SessionStatementProvider()
    {
    }

    public void close()
    {
        preparedStatementCache.clear();
    }

    public PreparedStatement prepare(String stmt, Session session)
    {
        PreparedStatement ps = preparedStatementCache.get(stmt);
        if (ps == null)
        {
            ps = session.prepare(stmt);
            preparedStatementCache.put(stmt, ps);
        }
        return ps;
    }
}