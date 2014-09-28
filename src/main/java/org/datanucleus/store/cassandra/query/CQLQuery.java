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

Contributors :
    barisergun75@gmail.com
 ***********************************************************************/
package org.datanucleus.store.cassandra.query;

import org.datanucleus.store.cassandra.pojo.ResultClassInfo;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import java.nio.ByteBuffer;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.store.cassandra.CassandraUtils;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * CQL query for Cassandra. Allows the user to execute a CQL query and return
 * the results in the form "List&lt;Object[]&gt;".
 */
public class CQLQuery extends AbstractJavaQuery {
	private static final long serialVersionUID = 2808968696540162104L;

	/**
	 * The compilation of the query for this datastore. Not applicable if
	 * totally in-memory.
	 */
	protected transient CassandraQueryCompilation datastoreCompilation;

	String cql;

	/**
	 * Constructs a new query instance that uses the given execution context.
	 * 
	 * @param storeMgr
	 *            StoreManager for this query
	 * @param ec
	 *            execution context
	 */
	public CQLQuery(StoreManager storeMgr, ExecutionContext ec) {
		this(storeMgr, ec, (CQLQuery) null);
	}

	/**
	 * Constructs a new query instance having the same criteria as the given
	 * query.
	 * 
	 * @param storeMgr
	 *            StoreManager for this query
	 * @param ec
	 *            execution context
	 * @param q
	 *            The query from which to copy criteria.
	 */
	public CQLQuery(StoreManager storeMgr, ExecutionContext ec, CQLQuery q) {
		super(storeMgr, ec);
		this.cql = q.cql;
	}

	/**
	 * Constructor for a JDOQL query where the query is specified using the
	 * "Single-String" format.
	 * 
	 * @param storeMgr
	 *            StoreManager for this query
	 * @param ec
	 *            execution context
	 * @param query
	 *            The query string
	 */
	public CQLQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
		super(storeMgr, ec);
		this.cql = query;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.datanucleus.store.query.AbstractJavaQuery#getSingleStringQuery()
	 */
	@Override
	public String getSingleStringQuery() {
		return cql;
	}

	@Override
	public void compileGeneric(Map parameterValues) {
	}

	@Override
	protected void compileInternal(Map parameterValues) {

	}

	@Override
	protected Object performExecute(Map parameters) {
		if (type == SELECT) {
			ManagedConnection mconn = getStoreManager().getConnection(ec);
			try {
				List results = new ArrayList();
				Session session = (Session) mconn.getConnection();

				long startTime = System.currentTimeMillis();
				if (NucleusLogger.QUERY.isDebugEnabled()) {
					NucleusLogger.QUERY.debug(Localiser.msg("021046", "CQL",
							getSingleStringQuery(), null));
				}

				Statement stmt = new SimpleStatement(cql);
				int fetchSize = this.getFetchPlan().getFetchSize();
				if (0 < fetchSize) {
					stmt.setFetchSize(fetchSize);
				}
				ResultSet rs = session.execute(stmt);

				// Datanucleus favors usage of byte[] as POJO for blob
				// dataStoreType
				// Cassandra datastax driver uses ByteBuffer for blob
				// dataStoreType
				TypeConverter typeConverter = storeMgr
						.getNucleusContext()
						.getTypeManager()
						.getTypeConverterForType(byte[].class, ByteBuffer.class);
				Class resultClazz = this.getResultClass();

				ResultClassInfo rci = null;
				if (null != resultClazz) {
					rci = CassandraUtils
							.getResultClassInfoFromColumnDefinitions(
									resultClazz, rs.getColumnDefinitions());

				}

				CassandraQueryResult queryResult = new CassandraQueryResult(
						this, rs);
				queryResult.initialise();
				Iterator<Object> iter = queryResult.iterator();
				while (iter.hasNext()) {
					Row row = (Row) iter.next();
					Object[] rowResult;
					if (null != rci) {
						rowResult = CassandraUtils.getObjectArrayFromRow(row,
								rs.getColumnDefinitions(),
								rci.getFieldsMatchingColumnIndexes(),
								typeConverter, rci.getFields().length);
						results.add(getResultWithQueryUtils(rowResult, rci));
					} else {
						rowResult = CassandraUtils.getObjectArrayFromRow(row,
								rs.getColumnDefinitions(),
								new ArrayList<Integer>(), typeConverter, rs
										.getColumnDefinitions().size());
						results.add(rowResult);// get raw result as Object[]
					}
				}
				if (NucleusLogger.QUERY.isDebugEnabled()) {
					NucleusLogger.QUERY.debug(Localiser.msg("021074", "CQL", ""
							+ (System.currentTimeMillis() - startTime)));
				}
				queryResult.close();

				return results;
			} finally {
				mconn.release();
			}

		} else if (type == BULK_DELETE || type == BULK_UPDATE) {
			// TODO
			throw new UnsupportedOperationException("Not yet implemented");
		} else {
			// TODO
			throw new UnsupportedOperationException("Not yet implemented");
		}

	}

	private Object getResultWithQueryUtils(Object[] result, ResultClassInfo rci) {
		return QueryUtils.createResultObjectUsingDefaultConstructorAndSetters(
				resultClass, rci.getFieldNames(), rci.getFields(), result);
	}

}
