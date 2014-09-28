/**********************************************************************
Copyright (c) 2014 Baris ERGUN and others. All rights reserved. 
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

package org.datanucleus.store.cassandra.query;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.Iterator;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.QueryManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class CassandraQueryResultWithResultCacheTest
{

    private CQLQuery mockCqlQuery;

    private ResultSet mockResultSet;

    private CassandraQueryResult cqlQueryResult;

    public CassandraQueryResultWithResultCacheTest()
    {
    }

    @BeforeClass
    public static void setUpClass()
    {

    }

    @AfterClass
    public static void tearDownClass()
    {
    }

    @Before
    public void setUp()
    {

        mockCqlQuery = mock(CQLQuery.class);
        mockResultSet = mock(ResultSet.class);

        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        when(mockCqlQuery.getExecutionContext()).thenReturn(mockExecutionContext);
        when(mockExecutionContext.getApiAdapter()).thenReturn(mock(ApiAdapter.class));
        when(mockCqlQuery.getStringExtensionProperty(Query.EXTENSION_RESULT_SIZE_METHOD, "last")).thenReturn("resultSizeMethod");
        when(mockCqlQuery.getBooleanExtensionProperty(Query.EXTENSION_LOAD_RESULTS_AT_COMMIT, true)).thenReturn(true);
        when(mockCqlQuery.useResultsCaching()).thenReturn(true);

        cqlQueryResult = new CassandraQueryResult(mockCqlQuery, mockResultSet);

        verify(mockCqlQuery, times(1)).getExecutionContext();
        verify(mockExecutionContext, times(1)).getApiAdapter();
        verify(mockCqlQuery, times(1)).getStringExtensionProperty(eq(Query.EXTENSION_RESULT_SIZE_METHOD), eq("last"));
        verify(mockCqlQuery, times(1)).getBooleanExtensionProperty(eq(Query.EXTENSION_LOAD_RESULTS_AT_COMMIT), eq(true));
        verify(mockCqlQuery, times(1)).useResultsCaching();
        verifyNoMoreInteractions(mockResultSet);

    }

    @After
    public void tearDown()
    {

    }

    @Test
    public void shouldCloseResultsIfNoMoreElementsOnInitialize()
    {
        Iterator<Row> mockRowIter = mock(Iterator.class);
        when(mockResultSet.iterator()).thenReturn(mockRowIter);
        when(mockRowIter.hasNext()).thenReturn(false);
        QueryManager mockQueryManager = mock(QueryManager.class);
        when(mockCqlQuery.getQueryManager()).thenReturn(mockQueryManager);
        FetchPlan mockFetchPlan = mock(FetchPlan.class);
        when(mockCqlQuery.getFetchPlan()).thenReturn(mockFetchPlan);
        when(mockFetchPlan.getFetchSize()).thenReturn(3);

        cqlQueryResult.initialise();

        verify(mockCqlQuery, times(1)).getFetchPlan();
        verify(mockFetchPlan, times(1)).getFetchSize();
        verify(mockResultSet, times(1)).iterator();
        verify(mockRowIter, times(1)).hasNext();
        verify(mockCqlQuery, times(1)).getQueryManager();
        verify(mockQueryManager, times(1)).addQueryResult(eq(mockCqlQuery), anyMap(), anyList());
        verifyNoMoreInteractions(mockResultSet, mockRowIter);

    }

    @Test
    public void shouldProcessRowsOneByOneOnInitializeWithGreedyFetch()
    {
        Iterator<Row> mockRowIter = mock(Iterator.class);
        when(mockResultSet.iterator()).thenReturn(mockRowIter);
        when(mockRowIter.hasNext()).thenAnswer(new Answer()
        {
            int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable
            {
                return count++ < 5;
            }
        });

        when(mockResultSet.one()).thenAnswer(new Answer()
        {
            int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable
            {
                if (count++ < 4)
                {
                    return mock(Row.class);
                }
                return null;
            }
        });
        FetchPlan mockFetchPlan = mock(FetchPlan.class);
        when(mockCqlQuery.getFetchPlan()).thenReturn(mockFetchPlan);
        when(mockFetchPlan.getFetchSize()).thenReturn(FetchPlan.FETCH_SIZE_GREEDY);

        cqlQueryResult.initialise();

        verify(mockCqlQuery, times(1)).getFetchPlan();
        verify(mockFetchPlan, times(1)).getFetchSize();
        verify(mockResultSet, times(2)).iterator();
        verify(mockRowIter, times(6)).hasNext();
        verify(mockResultSet, times(4)).one();
        verifyNoMoreInteractions(mockResultSet, mockRowIter);

    }

}
