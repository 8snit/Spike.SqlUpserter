using System;
using System.Collections.Generic;

namespace Spike.SqlUpserter
{
    public class SqlTableMapper<TObject>
    {
        private readonly IList<SqlColumnDescriptor<TObject>> _columns = new List<SqlColumnDescriptor<TObject>>();

        public IEnumerable<SqlColumnDescriptor<TObject>> Columns
        {
            get { return _columns; }
        }

        public int IndexOf(string columnName)
        {
            for (var index = 0; index < _columns.Count; index++)
            {
                if (string.Compare(_columns[index].Name, columnName, StringComparison.InvariantCultureIgnoreCase) == 0)
                {
                    return index;
                }
            }
            return -1;
        }

        public object GetValueAt(int index, TObject Object)
        {
            if (index < 0
                || index >= _columns.Count)
            {
                return null;
            }

            return _columns[index].ValueAccessor(Object);
        }

        public void Add(SqlColumnDescriptor<TObject> column)
        {
            if (IndexOf(column.Name) >= 0)
            {
                throw new Exception(string.Format("column '{0}' already exists", column.Name));
            }

            _columns.Add(column);
        }
    }

    public static class SqlTableMapperExtensions
    {
        public static void Add<TObject>(this SqlTableMapper<TObject> sqlTableMapper, Func<TObject, object> valueAccessor,
            string name, string sqlType, bool isDatabaseGenerated = false, bool isPrimaryKey = false,
            bool isEditable = true)
        {
            sqlTableMapper.Add(new SqlColumnDescriptor<TObject>
            {
                ValueAccessor = valueAccessor,
                Name = name,
                SqlType = sqlType,
                IsDatabaseGenerated = isDatabaseGenerated,
                IsPrimaryKey = isPrimaryKey,
                IsEditable = isEditable
            });
        }
    }
}