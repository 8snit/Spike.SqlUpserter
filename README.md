# SqlUpserter

SQL batching with SqlBulkCopy

### Introduction

Spike of a simple SQL batching component using SqlBulkCopy together with an automatically generated Stored Procedure for database merge.

### Usage

Provide the corresponding database mapping...

```c#
    var sqlTableMapper = new SqlTableMapper<Student>();
    sqlTableMapper.Add(student => student.Id, "id", "bigint", isPrimaryKey: true);
    sqlTableMapper.Add(student => student.FirstName, "firstname", "varchar(50)");
    sqlTableMapper.Add(student => student.LastName, "lastname", "varchar(50)");
    sqlTableMapper.Add(student => student.BirthDate, "birthdate", "datetime");
    sqlTableMapper.Add(student => student.IdentificationNumber, "idnumber", "varchar(10)", isEditable: false);
    sqlTableMapper.Add(student => student.ClassLevel, "classlevel", "int");
    return sqlTableMapper;
```

...and simple upload all data with the SqlUpserter component:

```c#
    var students = <sample data>
    using (var sqlConnection = DatabaseHelper.OpenSqlConnection(DatabaseName))
    {
        var sqlUpserter = new SqlUpserter<Student>(StudentMapper, TableName, students);
        sqlUpserter.Execute(sqlConnection);
	}
```


### Feedback
Welcome! Just raise an issue or send a pull request.

