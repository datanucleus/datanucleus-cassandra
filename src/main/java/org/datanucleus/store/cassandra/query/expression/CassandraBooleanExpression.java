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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.util.NucleusLogger;

/**
 * Representation of a boolean expression in Cassandra queries, and contains the CQL that it maps to.
 */
public class CassandraBooleanExpression extends CassandraExpression
{
    String cql = null;

    /**
     * Constructor when the expression represents a comparison, between the field expression and a literal.
     * @param leftExpr Left expression (CassandraFieldExpression or CassandraLiteral)
     * @param rightExpr Left expression (CassandraFieldExpression or CassandraLiteral)
     * @param op The operator (eq, noteq, lt, gt, etc)
     */
    public CassandraBooleanExpression(CassandraExpression leftExpr, CassandraExpression rightExpr, Expression.Operator op)
    {
        if (op == Expression.OP_EQ || op == Expression.OP_NOTEQ || op == Expression.OP_LT || op == Expression.OP_LTEQ || op == Expression.OP_GT || op == Expression.OP_GTEQ)
        {
            String leftCql = null;
            if (leftExpr instanceof CassandraFieldExpression)
            {
                leftCql = ((CassandraFieldExpression) leftExpr).getColumnName();
            }
            else if (leftExpr instanceof CassandraLiteral)
            {
                Object value = ((CassandraLiteral) leftExpr).getValue();
                if (value instanceof String || value instanceof Character)
                {
                    leftCql = "'" + value + "'";
                }
                else
                {
                    leftCql = "" + value;
                }
            }
            else
            {
                throw new NucleusException("Cannot create CassandraBooleanExpression with left argument of type " + leftExpr.getClass().getName());
            }

            String rightCql = null;
            if (rightExpr instanceof CassandraFieldExpression)
            {
                rightCql = ((CassandraFieldExpression) rightExpr).getColumnName();
            }
            else if (rightExpr instanceof CassandraLiteral)
            {
                Object value = ((CassandraLiteral) rightExpr).getValue();
                if (value instanceof String || value instanceof Character)
                {
                    rightCql = "'" + value + "'";
                }
                else
                {
                    rightCql = "" + value;
                }
            }
            else
            {
                throw new NucleusException("Cannot create CassandraBooleanExpression with right argument of type " + rightExpr.getClass().getName());
            }

            if (op == Expression.OP_NOTEQ)
            {
                // TODO In CQL there is no "NOT" operator!
                NucleusLogger.QUERY.warn("CQL has no NOT operator! so you need to change your query");
            }
            cql = leftCql + op.toString() + rightCql;
        }
        else if (op == Expression.OP_AND || op == Expression.OP_OR)
        {
            if (leftExpr instanceof CassandraBooleanExpression && rightExpr instanceof CassandraBooleanExpression)
            {
                cql = "(" + ((CassandraBooleanExpression) leftExpr).getCQL() + ")" + op.toString() + "(" + ((CassandraBooleanExpression) rightExpr).getCQL() + ")";
            }
            else
            {
                throw new NucleusException("Cannot create CassandraBooleanExpression with left=" + leftExpr.getClass().getName() + " right=" + rightExpr.getClass().getName());
            }
        }
        else
        {
            throw new NucleusException("Cannot create CassandraBooleanExpression with operator of " + op + " with this constructor");
        }
    }

    public String getCQL()
    {
        return cql;
    }

    public String toString()
    {
        return cql.toString();
    }
}