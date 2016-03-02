using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using NUnit.Framework;

namespace Spike.SqlUpserter.Tests
{
    [TestFixture]
    public class PerformanceTests
    {
        [SetUp]
        public void SetUp()
        {
            DatabaseHelper.CreateDatabase(DatabaseName, true);

            DatabaseHelper.CreateTable(DatabaseName, TableName, StudentMapper.Columns.ToArray());
        }

        [TearDown]
        public void TearDown()
        {
            DatabaseHelper.DropDatabase(DatabaseName);
        }

        private const string DatabaseName = "SqlUpserter_PerformanceTests";

        private const string TableName = "Students";

        private static readonly SqlTableMapper<Student> StudentMapper = StudentHelper.CreateSqlTableMapper();

        public static IEnumerable<int> Count()
        {
            return Enumerable.Range(1, 20).Select(i => (int) Math.Pow(2, i - 1));
        }

        [Test, TestCaseSource(nameof(Count))]
        public void TestPerformance(int count)
        {
            var students = StudentHelper.CreateSample(count).ToArray();
            var sqlTableMapper = StudentHelper.CreateSqlTableMapper();

            using (var sqlConnection = DatabaseHelper.OpenSqlConnection(DatabaseName))
            {
                var sw = new Stopwatch();
                sw.Start();
                var sqlUpserter = new SqlUpserter<Student>(sqlTableMapper, TableName, students);
                sqlUpserter.Execute(sqlConnection);

                sw.Stop();
                var path = Path.Combine(TestContext.CurrentContext.TestDirectory, "TestPerformance.txt");
                File.AppendAllText(path, string.Format("#{0}: {1}ms\r\n", count, sw.ElapsedMilliseconds));
            }
        }
    }
}