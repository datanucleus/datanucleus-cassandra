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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.schema.SchemaAwareStoreManager;

/**
 * StoreManager for persisting to Cassandra datastores.
 */
public class CassandraStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    /**
     * Constructor.
     * @param key Key for Cassandra stores
     * @param clr ClassLoader resolver
     * @param nucleusCtx Nucleus context
     * @param props Properties for the store manager
     */
    public CassandraStoreManager(String key, ClassLoaderResolver clr, NucleusContext nucleusContext, Map<String, Object> props)
    {
        super("cassandra", clr, nucleusContext, props);

    }

    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("DatastoreIdentity");
        set.add("ORM");
        set.add("TransactionIsolationLevel.read-committed");
        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#createSchema(java.util.Set, java.util.Properties)
     */
    @Override
    public void createSchema(Set<String> classNames, Properties props)
    {
        // TODO Implement this
        throw new UnsupportedOperationException("Dont currently support schema creation");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#deleteSchema(java.util.Set, java.util.Properties)
     */
    @Override
    public void deleteSchema(Set<String> classNames, Properties props)
    {
        // TODO Implement this
        throw new UnsupportedOperationException("Dont currently support schema deletion");
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#validateSchema(java.util.Set, java.util.Properties)
     */
    @Override
    public void validateSchema(Set<String> classNames, Properties props)
    {
        // TODO Implement this
        throw new UnsupportedOperationException("Dont currently support schema validation");
    }
}