using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Spike.SqlUpserter
{
    public class SqlUpserter<TObject> : IDataReader
        where TObject : class
    {
        private readonly SqlTableMapper<TObject> _sqlTableMapper;

        private readonly string _tableName;

        private IEnumerator<TObject> _enumerator;

        public SqlUpserter(SqlTableMapper<TObject> sqlTableMapper, string tableName, IEnumerable<TObject> objects)
        {
            _sqlTableMapper = sqlTableMapper;
            _tableName = tableName;
            _enumerator = objects.GetEnumerator();
        }

        private string TempTableName
        {
            get { return "#" + _tableName; }
        }

        private string CreateTempTableText
        {
            get
            {
                var sb = new StringBuilder();
                sb.Append("CREATE TABLE ");
                sb.Append(TempTableName);
                sb.Append(" (");
                sb.Append(string.Join(",", _sqlTableMapper.Columns.Select(column => column.Name + " " + column.SqlType)));
                sb.Append(")");
                return sb.ToString();
            }
        }

        public string DropTempTableText
        {
            get
            {
                var sb = new StringBuilder();
                sb.Append("DROP TABLE ");
                sb.Append(TempTableName);
                return sb.ToString();
            }
        }

        public string MergeTablesText
        {
            get
            {
                var primaryKeyColumnName =
                    _sqlTableMapper.Columns.Where(column => column.IsPrimaryKey)
                        .DefaultIfEmpty(_sqlTableMapper.Columns.First())
                        .First()
                        .Name;
                var sb = new StringBuilder();
                sb.Append("MERGE INTO ");
                sb.Append(_tableName);
                sb.Append(" AS t USING ");
                sb.Append(TempTableName);
                sb.Append(" AS s ON ");
                sb.Append("t.");
                sb.Append(primaryKeyColumnName);
                sb.Append("=s.");
                sb.Append(primaryKeyColumnName);
                sb.Append(" WHEN MATCHED THEN UPDATE SET ");
                sb.Append(string.Join(",",
                    _sqlTableMapper.Columns.Where(column => column.IsEditable && !column.IsDatabaseGenerated)
                        .Select(c => "t." + c.Name + "=s." + c.Name)));
                sb.Append(" WHEN NOT MATCHED THEN INSERT (");
                sb.Append(string.Join(",",
                    _sqlTableMapper.Columns.Where(column => !column.IsDatabaseGenerated).Select(column => column.Name)));
                sb.Append(") VALUES (");
                sb.Append(string.Join(",",
                    _sqlTableMapper.Columns.Where(column => !column.IsDatabaseGenerated)
                        .Select(column => "s." + column.Name)));
                sb.Append(");");
                return sb.ToString();
            }
        }

        public void Close()
        {
            Dispose();
        }

        public int Depth
        {
            get { return 1; }
        }

        public DataTable GetSchemaTable()
        {
            return null;
        }

        public bool IsClosed
        {
            get { return _enumerator == null; }
        }

        public bool NextResult()
        {
            return false;
        }

        public bool Read()
        {
            if (_enumerator == null)
            {
                throw new ObjectDisposedException("BulkCopier");
            }
            return _enumerator.MoveNext();
        }

        public int RecordsAffected
        {
            get { return -1; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public int FieldCount
        {
            get { return _sqlTableMapper.Columns.Count(); }
        }

        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            throw new NotImplementedException();
        }

        public int GetOrdinal(string name)
        {
            var index = _sqlTableMapper.IndexOf(name);
            return index;
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public object GetValue(int i)
        {
            if (_enumerator.Current == null)
            {
                return null;
            }

            var value = _sqlTableMapper.GetValueAt(i, _enumerator.Current);
            return value;
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }

        public object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        public object this[int i]
        {
            get { throw new NotImplementedException(); }
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_enumerator != null)
                {
                    _enumerator.Dispose();
                    _enumerator = null;
                }
            }
        }

        public void Execute(string connectionString)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                Execute(sqlConnection);
            }
        }

        public void Execute(SqlConnection sqlConnection)
        {
            try
            {
                new SqlCommand(CreateTempTableText, sqlConnection).ExecuteNonQuery();
            }
            catch (Exception)
            {
                //try drop and create
                new SqlCommand(DropTempTableText, sqlConnection).ExecuteNonQuery();
                new SqlCommand(CreateTempTableText, sqlConnection).ExecuteNonQuery();
            }

            var sqlBulkCopy = new SqlBulkCopy(sqlConnection)
            {
                DestinationTableName = TempTableName
            };
            foreach (var columnName in _sqlTableMapper.Columns.Select(column => column.Name))
            {
                sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
            }
            sqlBulkCopy.WriteToServer(this);

            new SqlCommand(MergeTablesText, sqlConnection).ExecuteNonQuery();

            new SqlCommand(DropTempTableText, sqlConnection).ExecuteNonQuery();
        }
    }
}