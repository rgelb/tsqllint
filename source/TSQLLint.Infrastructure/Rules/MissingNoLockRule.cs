using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSQLLint.Core.Interfaces;
using TSQLLint.Infrastructure.Rules.Common;

namespace TSQLLint.Infrastructure.Rules
{
    public class MissingNoLockRule : TSqlFragmentVisitor, ISqlRule
    {
        private readonly Action<string, string, int, int> errorCallback;

        private HashSet<string> cteNames = new HashSet<string>();

        public MissingNoLockRule(Action<string, string, int, int> errorCallback)
        {
            this.errorCallback = errorCallback;
        }

        public string RULE_NAME => "missing-nolock";

        public string RULE_TEXT => "Missing nolock found in a select statement";

        public int DynamicSqlStartColumn { get; set; }

        public int DynamicSqlStartLine { get; set; }

        public override void Visit(NamedTableReference node) {
            // do not apply rule to temp tables
            if (node.SchemaObject.BaseIdentifier.Value.Contains("#")) return;

            // skip system tables
            List<string> skipTables = new() { "objects"};
            if (skipTables.Any(t => t == node.SchemaObject.BaseIdentifier.Value.ToLower())) return;

            // if the hint exists - all good
            if (node.TableHints.Any(th => th.HintKind == TableHintKind.NoLock)) return;

            // scroll back through the token until we encounter a DML statement
            List<string> dmlStatements = new() { "select", "insert", "update", "delete", "merge" };
            for (int i = node.FirstTokenIndex - 1; i >= 0; i--) {

                var match = dmlStatements.FirstOrDefault(d => d == node.ScriptTokenStream[i].Text.ToLower());
                if (!string.IsNullOrWhiteSpace(match)) {
                    if (match == "select") {
                        // we have a table without a NOLOCK statement
                        errorCallback(RULE_NAME, RULE_TEXT, node.StartLine, GetColumnNumber(node));
                    }

                    return;
                }
            }
        }

        private int GetColumnNumber(TSqlFragment node)
        {
            return node.StartLine == DynamicSqlStartLine
                ? node.StartColumn + DynamicSqlStartColumn
                : node.StartColumn;
        }

        public override void Visit(TableReference node)
        {
            void ChildCallback(TSqlFragment childNode)
            {
                var dynamicSqlAdjustment = AdjustColumnForDymamicSQL(childNode);
                var tabsOnLine = ColumnNumberCalculator.CountTabsBeforeToken(childNode.StartLine, childNode.LastTokenIndex, childNode.ScriptTokenStream);
                var column = ColumnNumberCalculator.GetColumnNumberBeforeToken(tabsOnLine, childNode.ScriptTokenStream[childNode.FirstTokenIndex]);
                errorCallback(RULE_NAME, RULE_TEXT, childNode.StartLine, column + dynamicSqlAdjustment);
            }

            var childTableJoinVisitor = new ChildTableJoinVisitor();
            node.AcceptChildren(childTableJoinVisitor);

            if (!childTableJoinVisitor.TableJoined)
            {
                return;
            }

            var childTableAliasVisitor = new ChildTableAliasVisitor(ChildCallback, cteNames);
            node.AcceptChildren(childTableAliasVisitor);
        }

        private int AdjustColumnForDymamicSQL(TSqlFragment node)
        {
            return node.StartLine == DynamicSqlStartLine
                ? DynamicSqlStartColumn
                : 0;
        }

        public class ChildCommonTableExpressionVisitor : TSqlFragmentVisitor
        {
            public HashSet<string> CommonTableExpressionIdentifiers { get; } = new HashSet<string>();

            public override void Visit(CommonTableExpression node)
            {
                CommonTableExpressionIdentifiers.Add(node.ExpressionName.Value);
            }
        }

        public class ChildTableJoinVisitor : TSqlFragmentVisitor
        {
            public bool TableJoined { get; private set; }

            public override void Visit(JoinTableReference node)
            {
                TableJoined = true;
            }
        }

        public class ChildTableAliasVisitor : TSqlFragmentVisitor
        {
            private readonly Action<TSqlFragment> childCallback;

            public ChildTableAliasVisitor(Action<TSqlFragment> errorCallback, HashSet<string> cteNames)
            {
                CteNames = cteNames;
                childCallback = errorCallback;
            }

            public HashSet<string> CteNames { get; }

            public override void Visit(NamedTableReference node)
            {
                if (CteNames.Contains(node.SchemaObject.BaseIdentifier.Value))
                {
                    return;
                }

                if (node.Alias == null)
                {
                    childCallback(node);
                }
            }
        }
    }
}
