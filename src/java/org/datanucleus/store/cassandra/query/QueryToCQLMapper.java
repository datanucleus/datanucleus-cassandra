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
package org.datanucleus.store.cassandra.query;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.OrderExpression;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.store.cassandra.query.expression.CassandraBooleanExpression;
import org.datanucleus.store.cassandra.query.expression.CassandraExpression;
import org.datanucleus.store.cassandra.query.expression.CassandraFieldExpression;
import org.datanucleus.store.cassandra.query.expression.CassandraLiteral;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Mapper for converting a generic query into CQL.
 */
public class QueryToCQLMapper extends AbstractExpressionEvaluator
{
    final ExecutionContext ec;

    final String candidateAlias;

    final AbstractClassMetaData candidateCmd;

    final Table table;

    final Query query;

    final QueryCompilation compilation;

    /** Input parameter values, keyed by the parameter name. Will be null if compiled pre-execution. */
    final Map parameters;

    /** Positional parameter that we are up to (-1 implies not being used). */
    int positionalParamNumber = -1;

    /** State variable for the component being compiled. */
    CompilationComponent compileComponent;

    /** Whether the filter clause is completely evaluatable in the datastore. */
    boolean filterComplete = true;

    /** Whether the result clause is completely evaluatable in the datastore. */
    boolean resultComplete = true;

    /** Whether the order clause is completely evaluatable in the datastore. */
    boolean orderComplete = true;

    boolean precompilable = true;

    /** Stack of expressions, used during compilation process. */
    Stack<CassandraExpression> stack = new Stack();

    String cql = null;

    public QueryToCQLMapper(QueryCompilation compilation, Map parameters, AbstractClassMetaData cmd, ExecutionContext ec, Query q, Table table)
    {
        this.ec = ec;
        this.query = q;
        this.compilation = compilation;
        this.parameters = parameters;
        this.candidateCmd = cmd;
        this.candidateAlias = compilation.getCandidateAlias();
        this.table = table;
    }

    public String getCQL()
    {
        return cql;
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public boolean isResultComplete()
    {
        return resultComplete;
    }

    public boolean isOrderComplete()
    {
        return orderComplete;
    }

    public boolean isPrecompilable()
    {
        return precompilable;
    }

    public void compile()
    {
        String filterCql = compileFilter();
        String resultCQL = compileResult();
        compileGrouping();
        compileHaving();
        String orderCQL = compileOrdering();
        // TODO Compile range also since CQL has "LIMIT n" (but no offset)

        // Build CQL for this query
        StringBuilder str = new StringBuilder();
        str.append("SELECT ");
        if (resultCQL != null)
        {
            str.append(resultCQL);
        }
        else
        {
            str.append("*");
        }

        if (table.getSchemaName() != null)
        {
            str.append(" FROM ").append(table.getSchemaName()).append('.').append(table.getIdentifier());
        }
        else
        {
            str.append(" FROM ").append(table.getIdentifier());
        }

        if (filterCql != null)
        {
            str.append(" WHERE ").append(filterCql);
        }

        if (orderCQL != null)
        {
            str.append(" ORDER BY ").append(orderCQL);
        }

        // TODO Support grouping, having
        cql = str.toString();
    }

    /**
     * Method to compile the FILTER clause of the query
     * @return The CQL for the filter
     */
    protected String compileFilter()
    {
        if (compilation.getExprFilter() != null)
        {
            compileComponent = CompilationComponent.FILTER;

            String cql = null;
            try
            {
                compilation.getExprFilter().evaluate(this);
                CassandraExpression filterExpr = stack.pop();
                cql = ((CassandraBooleanExpression)filterExpr).getCQL();
            }
            catch (Exception e)
            {
                // Impossible to compile all to run in the datastore, so just exit
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug("Compilation of filter to be evaluated completely in-datastore was impossible : " + e.getMessage());
                }
                filterComplete = false;
            }

            compileComponent = null;
            return cql;
        }
        return null;
    }

    /**
     * Method to compile the RESULT clause of the query
     * @return The CQL for the result
     */
    protected String compileResult()
    {
        if (compilation.getExprResult() != null)
        {
            // TODO Implement this
        }
        return null;
    }
    /**
     * Method to compile the GROUPING clause of the query
     */
    protected void compileGrouping()
    {
        if (compilation.getExprFilter() != null)
        {
            // TODO Implement this
        }
    }
    /**
     * Method to compile the HAVING clause of the query
     */
    protected void compileHaving()
    {
        if (compilation.getExprHaving() != null)
        {
            // TODO Implement this
        }
    }
    /**
     * Method to compile the ORDERING clause of the query
     * @return The CQL for the result
     */
    protected String compileOrdering()
    {
        if (compilation.getExprOrdering() != null)
        {
            StringBuilder orderStr = new StringBuilder();
            compileComponent = CompilationComponent.ORDERING;
            Expression[] orderingExpr = compilation.getExprOrdering();
            try
            {
                for (int i = 0; i < orderingExpr.length; i++)
                {
                    OrderExpression orderExpr = (OrderExpression) orderingExpr[i];
                    CassandraFieldExpression orderMongoExpr = (CassandraFieldExpression) orderExpr.getLeft().evaluate(this);
                    String orderDir = orderExpr.getSortOrder();
                    int direction = ((orderDir == null || orderDir.equals("ascending")) ? 1 : -1);
                    NucleusLogger.QUERY.debug(">> TODO Need to process " + orderExpr);
                    if (orderStr.length() > 0)
                    {
                        orderStr.append(',');
                    }
                    orderStr.append(orderMongoExpr.getColumnName()).append(" ");
                    orderStr.append(direction == 1 ? "ASC" : "DESC");
                }
            }
            catch (Exception e)
            {
                // Impossible to compile all to run in the datastore, so just exit
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug("Compilation of order to be evaluated completely in-datastore was impossible : " + e.getMessage());
                }
                orderComplete = false;
            }
            compileComponent = null;
            if (orderComplete && orderStr.length() > 0)
            {
                return orderStr.toString();
            }
        }
        return null;
    }

    @Override
    protected Object processAndExpression(Expression expr)
    {
        CassandraExpression right = stack.pop();
        CassandraExpression left = stack.pop();
        CassandraBooleanExpression boolExpr = new CassandraBooleanExpression(left, right, expr.getOperator());
        stack.push(boolExpr);
        return boolExpr;
    }

    @Override
    protected Object processOrExpression(Expression expr)
    {
        CassandraExpression right = stack.pop();
        CassandraExpression left = stack.pop();
        CassandraBooleanExpression boolExpr = new CassandraBooleanExpression(left, right, expr.getOperator());
        stack.push(boolExpr);
        return boolExpr;
    }

    @Override
    protected Object processEqExpression(Expression expr)
    {
        CassandraExpression right = stack.pop();
        CassandraExpression left = stack.pop();
        CassandraBooleanExpression boolExpr = new CassandraBooleanExpression(left, right, expr.getOperator());
        stack.push(boolExpr);
        return boolExpr;
    }

    @Override
    protected Object processNoteqExpression(Expression expr)
    {
        CassandraExpression right = stack.pop();
        CassandraExpression left = stack.pop();
        CassandraBooleanExpression boolExpr = new CassandraBooleanExpression(left, right, expr.getOperator());
        stack.push(boolExpr);
        return boolExpr;
    }

    @Override
    protected Object processGtExpression(Expression expr)
    {
        CassandraExpression right = stack.pop();
        CassandraExpression left = stack.pop();
        CassandraBooleanExpression boolExpr = new CassandraBooleanExpression(left, right, expr.getOperator());
        stack.push(boolExpr);
        return boolExpr;
    }

    @Override
    protected Object processGteqExpression(Expression expr)
    {
        CassandraExpression right = stack.pop();
        CassandraExpression left = stack.pop();
        CassandraBooleanExpression boolExpr = new CassandraBooleanExpression(left, right, expr.getOperator());
        stack.push(boolExpr);
        return boolExpr;
    }

    @Override
    protected Object processLtExpression(Expression expr)
    {
        CassandraExpression right = stack.pop();
        CassandraExpression left = stack.pop();
        CassandraBooleanExpression boolExpr = new CassandraBooleanExpression(left, right, expr.getOperator());
        stack.push(boolExpr);
        return boolExpr;
    }

    @Override
    protected Object processLteqExpression(Expression expr)
    {
        CassandraExpression right = stack.pop();
        CassandraExpression left = stack.pop();
        CassandraBooleanExpression boolExpr = new CassandraBooleanExpression(left, right, expr.getOperator());
        stack.push(boolExpr);
        return boolExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processPrimaryExpression(org.datanucleus.query.expression.PrimaryExpression)
     */
    @Override
    protected Object processPrimaryExpression(PrimaryExpression expr)
    {
        Expression left = expr.getLeft();
        if (left == null)
        {
            CassandraExpression memberExpr = getExpressionForPrimary(expr);
            if (memberExpr == null)
            {
                if (compileComponent == CompilationComponent.FILTER)
                {
                    filterComplete = false;
                }
                else if (compileComponent == CompilationComponent.RESULT)
                {
                    resultComplete = false;
                }
                NucleusLogger.QUERY.debug(">> Primary " + expr + " is not stored in this table, so unexecutable in datastore");
            }
            else
            {
                stack.push(memberExpr);
                return memberExpr;
            }
        }

        // TODO Auto-generated method stub
        return super.processPrimaryExpression(expr);
    }

    protected CassandraExpression getExpressionForPrimary(PrimaryExpression primExpr)
    {
        List<String> tuples = primExpr.getTuples();
        if (tuples == null || tuples.isEmpty())
        {
            return null;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        MetaDataManager mmgr = ec.getMetaDataManager();
        AbstractClassMetaData cmd = candidateCmd;
        boolean firstTuple = true;
        Iterator<String> iter = tuples.iterator();
        while (iter.hasNext())
        {
            String name = iter.next();
            if (firstTuple && name.equals(candidateAlias))
            {
                cmd = candidateCmd;
            } 
            else
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForMember(name);
                if (mmd != null)
                {
                    RelationType relationType = mmd.getRelationType(ec.getClassLoaderResolver());
                    if (relationType == RelationType.NONE)
                    {
                        if (iter.hasNext())
                        {
                            throw new NucleusUserException("Query has reference to " + StringUtils.collectionToString(tuples) + " yet " + name + " is a non-relation field!");
                        }
                    }

                    if (relationType != RelationType.NONE && MetaDataUtils.isMemberEmbedded(mmd, relationType, clr, mmgr))
                    {
                        if (RelationType.isRelationSingleValued(relationType))
                        {
                            // Embedded 1-1 TODO Support this
                        }
                        else if (RelationType.isRelationMultiValued(relationType))
                        {
                            // Embedded 1-N TODO Support this
                        }
                    }
                    else
                    {
                        if (relationType == RelationType.NONE)
                        {
                            // TODO Support multi-column mappings
                            MemberColumnMapping mapping = table.getMemberColumnMappingForMember(mmd);
                            if (mmd.getIndexMetaData() == null)
                            {
                                throw new NucleusUserException("Attempt to refer to " + mmd.getFullFieldName() + " in " + compileComponent + 
                                    " yet this is not indexed. Must be indexed to evaluate in datastore");
                            }
                            return new CassandraFieldExpression(mapping.getColumn(0).getIdentifier(), mmd);
                        }
                        else
                        {
                            // TODO Cater for relations?
                        }
                    }
                }
            }
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processParameterExpression(org.datanucleus.query.expression.ParameterExpression)
     */
    @Override
    protected Object processParameterExpression(ParameterExpression expr)
    {
        // Extract the parameter value (if set)
        Object paramValue = null;
        boolean paramValueSet = false;
        if (parameters != null && !parameters.isEmpty())
        {
            // Check if the parameter has a value
            if (parameters.containsKey(expr.getId()))
            {
                // Named parameter
                paramValue = parameters.get(expr.getId());
                paramValueSet = true;
            }
            else if (parameters.containsKey(expr.getId()))
            {
                // Positional parameter, but already encountered
                paramValue = parameters.get(expr.getId());
                paramValueSet = true;
            }
            else
            {
                // Positional parameter, not yet encountered
                int position = positionalParamNumber;
                if (positionalParamNumber < 0)
                {
                    position = 0;
                }
                if (parameters.containsKey(Integer.valueOf(position)))
                {
                    paramValue = parameters.get(Integer.valueOf(position));
                    paramValueSet = true;
                    positionalParamNumber = position+1;
                }
            }
        }
        if (paramValueSet)
        {
            CassandraLiteral lit = new CassandraLiteral(paramValue);
            stack.push(lit);
            precompilable = false;
            return lit;
        }

        // TODO Auto-generated method stub
        return super.processParameterExpression(expr);
    }

    @Override
    protected Object processLiteral(Literal expr)
    {
        Object litValue = expr.getLiteral();
        CassandraLiteral lit = new CassandraLiteral(litValue);
        stack.push(lit);
        return lit;
    }
}