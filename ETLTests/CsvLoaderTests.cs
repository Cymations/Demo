using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ETL;
using Xunit;

namespace ETLTests
{
    public class CsvLoaderTests : IDisposable
    {
        private readonly string _outputPath;
        private readonly string _rejectPath;

        public CsvLoaderTests()
        {
            _outputPath = Path.GetTempFileName();
            _rejectPath = Path.GetTempFileName();
        }

        public static IEnumerable<object[]> LoadData => new List<object[]>
        {
            new object[]
            {
                new[]
                {
                    new CsvTransformResult
                    {
                        ValidRows = { new[] { "A", "B" }, new[] { "C", "D" } },
                        InvalidRows = { (1, new[] { "X", "Y" }, new[] { "ERR1" }) }
                    }
                },
                new[] { "A,B", "C,D" },
                new[] { "1,X,Y,ERR1" }
            },
            new object[]
            {
                new[]
                {
                    new CsvTransformResult
                    {
                        ValidRows = { },
                        InvalidRows = { (2, new[] { "Z" }, new[] { "ERR2", "ERR3" }) }
                    }
                },
                Array.Empty<string>(),
                new[] { "2,Z,ERR2|ERR3" }
            }
        };

        [Theory]
        [MemberData(nameof(LoadData))]
        public void Load_WritesExpectedFiles(CsvTransformResult[] input, string[] expectedOutput, string[] expectedReject)
        {
            var loader = new CsvLoader(_outputPath, _rejectPath);
            loader.Load(input);
            var outputLines = File.ReadAllLines(_outputPath);
            var rejectLines = File.ReadAllLines(_rejectPath);
            Assert.Equal(expectedOutput, outputLines);
            Assert.Equal(expectedReject, rejectLines);
        }

        public void Dispose()
        {
            try { File.Delete(_outputPath); } catch { }
            try { File.Delete(_rejectPath); } catch { }
        }
    }
}
