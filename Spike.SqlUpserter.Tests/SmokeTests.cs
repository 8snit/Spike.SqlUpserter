using System.Data.SqlClient;
using System.Linq;
using NUnit.Framework;

namespace Spike.SqlUpserter.Tests
{
    [TestFixture]
    public class SmokeTests
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

        private const string DatabaseName = "SqlUpserter_SmokeTests";

        private const string TableName = "Students";

        private static readonly SqlTableMapper<Student> StudentMapper = StudentHelper.CreateSqlTableMapper();

        [Test]
        public void TestInsert()
        {
            var students = StudentHelper.CreateSample(10).ToArray();
            using (var sqlConnection = DatabaseHelper.OpenSqlConnection(DatabaseName))
            {
                var sqlUpserter = new SqlUpserter<Student>(StudentMapper, TableName, students);
                sqlUpserter.Execute(sqlConnection);

                var selectText = "SELECT " + string.Join(",", StudentMapper.Columns.Select(column => column.Name)) +
                                 " FROM " + TableName;
                var reader = new SqlCommand(selectText, sqlConnection).ExecuteReader();
                var row = 0;
                while (reader.Read())
                {
                    var student = students[row];
                    for (var column = 0; column < reader.FieldCount; column++)
                    {
                        var expected = StudentMapper.GetValueAt(column, student);
                        var actual = reader.GetValue(column);
                        Assert.AreEqual(expected, actual);
                    }
                    row++;
                }
            }
        }

        [Test]
        public void TestUpdate()
        {
            var students = StudentHelper.CreateSample(10).ToArray();
            using (var sqlConnection = DatabaseHelper.OpenSqlConnection(DatabaseName))
            {
                var sqlUpserter1 = new SqlUpserter<Student>(StudentMapper, TableName, students);
                sqlUpserter1.Execute(sqlConnection);

                var expectedChanges = 0;
                foreach (var student in students)
                {
                    if (student.Id%2 == 0)
                    {
                        student.FirstName = student.FirstName + "_Changed";
                        expectedChanges++;
                    }
                }

                var sqlUpserter2 = new SqlUpserter<Student>(StudentMapper, TableName, students);
                sqlUpserter2.Execute(sqlConnection);

                var selectText = "SELECT " + string.Join(",", StudentMapper.Columns.Select(column => column.Name)) +
                                 " FROM " + TableName;
                var reader = new SqlCommand(selectText, sqlConnection).ExecuteReader();
                var row = 0;
                var currentChanges = 0;
                while (reader.Read())
                {
                    var student = students[row];
                    for (var column = 0; column < reader.FieldCount; column++)
                    {
                        var expected = StudentMapper.GetValueAt(column, student);
                        var actual = reader.GetValue(column);
                        if (actual.ToString().EndsWith("_Changed"))
                        {
                            currentChanges++;
                        }
                        Assert.AreEqual(expected, actual);
                    }
                    row++;
                }
                Assert.AreEqual(expectedChanges, currentChanges);
            }
        }
    }
}