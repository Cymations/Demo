using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using ETL;
using Xunit;

namespace ETLTests
{
    public class CsvPipelineTests
    {
        [Theory]
        [InlineData("20260406-0042", "20260406-0042")]
        [InlineData(" 20260406-0001 ", "20260406-0001")]
        public void BatchIdParser_ValidInputs_Normalized(string input, string expected)
        {
            var result = BatchIdParser.Validate(input);
            Assert.True(result.IsValid);
            Assert.Equal(expected, result.Normalized);
            Assert.Empty(result.ErrorCodes);
        }

        [Theory]
        [InlineData("20260406-42", new[] { "FORMAT" })]
        [InlineData("20261301-0001", new[] { "DATE" })]
        [InlineData("20260406-0000", new[] { "SEQUENCE" })]
        [InlineData("20260406-10000", new[] { "FORMAT" })]
        [InlineData("badformat", new[] { "FORMAT" })]
        public void BatchIdParser_InvalidInputs_ErrorCodes(string input, string[] expectedErrors)
        {
            var result = BatchIdParser.Validate(input);
            Assert.False(result.IsValid);
            foreach (var expected in expectedErrors)
            {
                Assert.Contains(expected, result.ErrorCodes);
            }
        }

        [Fact]
        public void CsvExtractor_ExtractsRows_Correctly()
        {
            var csv = "a,b,c\n1,2,3\n4,5,6";
            var path = Path.GetTempFileName();
            File.WriteAllText(path, csv);
            var extractor = new CsvExtractor();
            var rows = extractor.Extract(path).ToList();
            Assert.Equal(new[] { "a", "b", "c" }, rows[0]);
            Assert.Equal(new[] { "1", "2", "3" }, rows[1]);
            Assert.Equal(new[] { "4", "5", "6" }, rows[2]);
            File.Delete(path);
        }

        [Fact]
        public void CsvTransformer_ValidAndInvalidRows()
        {
            var rows = new List<string[]>
            {
                new[] { "20260406-0042", "foo" }, // valid
                new[] { "badformat", "bar" },     // invalid
                new[] { "20261301-0001", "baz" }  // invalid date
            };
            var transformer = new CsvTransformer(0);
            var result = transformer.Transform(rows);
            Assert.Single(result.ValidRows);
            Assert.Equal("20260406-0042", result.ValidRows[0][0]);
            Assert.Equal(2, result.InvalidRows.Count);
            Assert.Equal("FORMAT", result.InvalidRows[0].ErrorCodes[0]);
            Assert.Equal("DATE", result.InvalidRows[1].ErrorCodes[0]);
        }

        [Fact]
        public void CsvLoader_WritesValidAndInvalidRows()
        {
            var validRows = new List<string[]> { new[] { "20260406-0042", "foo" } };
            var invalidRows = new List<(int, string[], string[])>
            {
                (2, new[] { "badformat", "bar" }, new[] { "FORMAT" }),
                (3, new[] { "20261301-0001", "baz" }, new[] { "DATE" })
            };
            var transformResult = new CsvTransformResult();
            transformResult.ValidRows.AddRange(validRows);
            transformResult.InvalidRows.AddRange(invalidRows);
            var outputPath = Path.GetTempFileName();
            var rejectPath = Path.GetTempFileName();
            var loader = new CsvLoader(outputPath, rejectPath);
            loader.Load(new[] { transformResult });
            var outputLines = File.ReadAllLines(outputPath);
            var rejectLines = File.ReadAllLines(rejectPath);
            Assert.Single(outputLines);
            Assert.Equal("20260406-0042,foo", outputLines[0]);
            Assert.Equal(2, rejectLines.Length);
            Assert.Contains("2,badformat,bar,FORMAT", rejectLines[0].Replace("|", ","));
            Assert.Contains("3,20261301-0001,baz,DATE", rejectLines[1].Replace("|", ","));
            File.Delete(outputPath);
            File.Delete(rejectPath);
        }
    }
}
