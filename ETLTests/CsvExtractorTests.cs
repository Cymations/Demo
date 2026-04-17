using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ETL;
using Xunit;

namespace ETLTests
// Suppress nullable warnings for test null argument cases
#pragma warning disable CS8604, CS8625
{
    public class CsvExtractorTests : IDisposable
    {
        private readonly List<string> _tempFiles = new();

        public static IEnumerable<object[]> ValidExtractData => new List<object[]>
        {
            new object[] { "col1,col2\nval1,val2\nval3,val4", true, new[] { new[] { "val1", "val2" }, new[] { "val3", "val4" } }, new[] { "col1", "col2" } },
            new object[] { "a,b\n1,2", false, new[] { new[] { "a", "b" }, new[] { "1", "2" } }, null },
            new object[] { "x\ny", false, new[] { new[] { "x" }, new[] { "y" } }, null },
        };

        public static IEnumerable<object[]> InvalidExtractData => new List<object[]>
        {
            new object[] { null, typeof(ArgumentNullException) },
            new object[] { "", typeof(InvalidDataException) },
            new object[] { "nonexistent.csv", typeof(FileNotFoundException) },
        };

        [Theory]
        [MemberData(nameof(ValidExtractData))]
        public void Extract_ValidCsv_ReturnsRowsAndHeader(string csvContent, bool hasHeader, string[][] expectedRows, string[] expectedHeader)
        {
            var filePath = CreateTempFile(csvContent);
            var extractor = new CsvExtractor();
            var rows = extractor.Extract(filePath, hasHeader).ToArray();
            Assert.Equal(expectedRows, rows);
            if (hasHeader)
                Assert.Equal(expectedHeader, extractor.Header);
            else
                Assert.Null(extractor.Header);
        }

        [Theory]
        [MemberData(nameof(InvalidExtractData))]
        public void Extract_InvalidInput_Throws(string filePath, Type expectedException)
        {
            var extractor = new CsvExtractor();
            if (filePath != null && filePath != "nonexistent.csv")
                filePath = CreateTempFile(filePath); // For empty string test
            Assert.Throws(expectedException, () => extractor.Extract(filePath, false).ToList());
        }

        private string CreateTempFile(string content)
        {
            var path = Path.GetTempFileName();
            File.WriteAllText(path, content);
            _tempFiles.Add(path);
            return path;
        }

        public void Dispose()
        {
            foreach (var file in _tempFiles)
            {
                try { File.Delete(file); } catch { }
            }
        }
// Restore warnings
#pragma warning restore CS8604, CS8625
    }
}
