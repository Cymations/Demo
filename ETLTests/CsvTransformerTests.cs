using System.Collections.Generic;
using System.Linq;
using ETL;
using Xunit;

namespace ETLTests
{
    public class CsvTransformerTests
    {
        public static IEnumerable<object[]> ValidBatchIdData => new List<object[]>
        {
            new object[] { new[] { "abcde" }, 0, new[] { "ABCDE" } },
            new object[] { new[] { " 12345 " }, 0, new[] { "12345" } },
            new object[] { new[] { "A1B2C3" }, 0, new[] { "A1B2C3" } },
            new object[] { new[] { "row", "batchid" }, 1, new[] { "row", "BATCHID" } },
        };

        public static IEnumerable<object[]> InvalidBatchIdData => new List<object[]>
        {
            new object[] { new[] { "" }, 0, new[] { "EMPTY_BATCHID" } },
            new object[] { new[] { "abc" }, 0, new[] { "INVALID_FORMAT" } },
            new object[] { new[] { "!@#$%" }, 0, new[] { "INVALID_FORMAT" } },
            new object[] { new[] { "row", "" }, 1, new[] { "EMPTY_BATCHID" } },
            new object[] { new[] { "row", "abc" }, 1, new[] { "INVALID_FORMAT" } },
        };

        [Theory]
        [MemberData(nameof(ValidBatchIdData))]
        public void Transform_ValidBatchId_NormalizesBatchId(string[] row, int batchIdCol, string[] expected)
        {
            var transformer = new CsvTransformer(batchIdCol);
            var input = new List<string[]> { row };
            var result = transformer.Transform(input);
            Assert.Single(result.ValidRows);
            Assert.Equal(expected, result.ValidRows[0]);
            Assert.Empty(result.InvalidRows);
        }

        [Theory]
        [MemberData(nameof(InvalidBatchIdData))]
        public void Transform_InvalidBatchId_ReturnsErrorCodes(string[] row, int batchIdCol, string[] expectedErrorCodes)
        {
            var transformer = new CsvTransformer(batchIdCol);
            var input = new List<string[]> { row };
            var result = transformer.Transform(input);
            Assert.Empty(result.ValidRows);
            Assert.Single(result.InvalidRows);
            var (_, _, errorCodes) = result.InvalidRows[0];
            Assert.Equal(expectedErrorCodes, errorCodes);
        }
    }
}
