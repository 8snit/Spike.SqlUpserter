using System;

namespace Spike.SqlUpserter.Tests
{
    public class Student
    {
        public long Id { get; set; }

        public string FirstName { get; set; }

        public string LastName { get; set; }

        public DateTime BirthDate { get; set; }

        public string IdentificationNumber { get; set; }

        public int ClassLevel { get; set; }
    }
}