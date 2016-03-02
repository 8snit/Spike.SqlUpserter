using System;
using System.Collections.Generic;
using System.Linq;

namespace Spike.SqlUpserter.Tests
{
    public static class StudentHelper
    {
        public static SqlTableMapper<Student> CreateSqlTableMapper()
        {
            var sqlTableMapper = new SqlTableMapper<Student>();
            sqlTableMapper.Add(student => student.Id, "id", "bigint", isPrimaryKey: true);
            sqlTableMapper.Add(student => student.FirstName, "firstname", "varchar(50)");
            sqlTableMapper.Add(student => student.LastName, "lastname", "varchar(50)");
            sqlTableMapper.Add(student => student.BirthDate, "birthdate", "datetime");
            sqlTableMapper.Add(student => student.IdentificationNumber, "idnumber", "varchar(10)", isEditable: false);
            sqlTableMapper.Add(student => student.ClassLevel, "classlevel", "int");
            return sqlTableMapper;
        }

        public static IEnumerable<Student> CreateSample(int count)
        {
            return Enumerable.Range(1, count)
                .Select(i => new Student
                {
                    Id = i,
                    FirstName = "FN" + i,
                    LastName = "LN" + i,
                    BirthDate = DateTime.Now.AddYears(-10).AddDays(-i%365).Date,
                    ClassLevel = i%10 + 1,
                    IdentificationNumber = Guid.NewGuid().ToString().Substring(0, 10)
                });
        }
    }
}