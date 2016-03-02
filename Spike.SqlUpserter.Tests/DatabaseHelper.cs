using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Spike.SqlUpserter.Tests
{
    public static class DatabaseHelper
    {
        private const string ConnectionString = @"Server=(localdb)\v11.0;Integrated Security=true";

        public static void CreateDatabase(string databaseName, bool forceNew = false)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                try
                {
                    sqlConnection.Open();
                    new SqlCommand("CREATE DATABASE " + databaseName, sqlConnection).ExecuteNonQuery();
                }
                catch (Exception)
                {
                    //try drop and create
                    if (forceNew)
                    {
                        DropDatabase(databaseName);
                        new SqlCommand("CREATE DATABASE " + databaseName, sqlConnection).ExecuteNonQuery();
                    }
                }
            }
        }

        public static void DropDatabase(string databaseName)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                try
                {
                    sqlConnection.Open();

                    new SqlCommand("ALTER DATABASE " + databaseName + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
                        sqlConnection).ExecuteNonQuery();
                    new SqlCommand("DROP DATABASE " + databaseName, sqlConnection).ExecuteNonQuery();
                }
                catch (Exception)
                {
                    // ignore
                }
            }
        }

        public static void CreateTable<TObject>(string databaseName, string tableName,
            SqlColumnDescriptor<TObject>[] columns)
        {
            using (var sqlConnection = OpenSqlConnection(databaseName))
            {
                try
                {
                    try
                    {
                        new SqlCommand("DROP TABLE " + tableName, sqlConnection).ExecuteNonQuery();
                    }
                    catch (Exception)
                    {
                        // ignore
                    }
                    var createText = "CREATE TABLE " + tableName + " (" +
                                     string.Join(",", columns.Select(column => column.Name + " " + column.SqlType)) +
                                     ")";
                    new SqlCommand(createText, sqlConnection).ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex.Message);
                    throw;
                }
            }
        }

        public static SqlConnection OpenSqlConnection(string databaseName)
        {
            try
            {
                var sqlConnection = new SqlConnection(ConnectionString);
                sqlConnection.Open();
                sqlConnection.ChangeDatabase(databaseName);
                return sqlConnection;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
        }
    }
}