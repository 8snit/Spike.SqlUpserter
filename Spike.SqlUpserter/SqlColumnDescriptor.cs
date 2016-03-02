using System;

namespace Spike.SqlUpserter
{
    public class SqlColumnDescriptor<TObject>
    {
        public Func<TObject, object> ValueAccessor { get; set; }

        public string Name { get; set; }

        public string SqlType { get; set; }

        public bool IsDatabaseGenerated { get; set; }

        public bool IsPrimaryKey { get; set; }

        public bool IsEditable { get; set; }
    }
}