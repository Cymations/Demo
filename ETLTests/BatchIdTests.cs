using System;
using ETL;
using Xunit;

namespace ETLTests
{
    public class BatchIdTests
    {
        [Theory]
        [InlineData("20240417-0001", 2024, 4, 17, 1)]
        [InlineData("19991231-1234", 1999, 12, 31, 1234)]
        public void TryParse_ValidToken_ReturnsTrueAndCorrectValues(string token, int year, int month, int day, int seq)
        {
            var result = BatchId.TryParse(token, out var batchId);
            Assert.True(result);
            Assert.NotNull(batchId);
            Assert.Equal(new DateTime(year, month, day), batchId.Date);
            Assert.Equal(seq, batchId.Sequence);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("202404170001")]
        [InlineData("20240417-000")]
        [InlineData("20240417-00001")]
        [InlineData("20240417-ABCD")]
        [InlineData("20241301-0001")]
        [InlineData("20240230-0001")]
        [InlineData("20240417-0000")]
        [InlineData("20240417-10000")]
        public void TryParse_InvalidToken_ReturnsFalse(string token)
        {
            var result = BatchId.TryParse(token, out var batchId);
            Assert.False(result);
            Assert.Null(batchId);
        }

        [Theory]
        [InlineData(" 20240417-0001 ", "20240417-0001")]
        [InlineData("20240417-0001", "20240417-0001")]
        [InlineData(null, null)]
        public void NormalizeToken_TrimsWhitespace(string input, string expected)
        {
            var normalized = BatchId.NormalizeToken(input);
            Assert.Equal(expected, normalized);
        }

        [Theory]
        [InlineData(null, BatchIdParseResult.Missing)]
        [InlineData("", BatchIdParseResult.Missing)]
        [InlineData("202404170001", BatchIdParseResult.InvalidFormat)]
        [InlineData("20240417-000", BatchIdParseResult.InvalidFormat)]
        [InlineData("20240417-00001", BatchIdParseResult.InvalidFormat)]
        [InlineData("20240417-ABCD", BatchIdParseResult.InvalidSequence)]
        [InlineData("20241301-0001", BatchIdParseResult.InvalidDate)]
        [InlineData("20240230-0001", BatchIdParseResult.InvalidDate)]
        [InlineData("20240417-0000", BatchIdParseResult.InvalidSequence)]
        [InlineData("20240417-10000", BatchIdParseResult.InvalidFormat)]
        [InlineData("20240417-0001", BatchIdParseResult.Valid)]
        public void Validate_ReturnsExpectedResult(string token, BatchIdParseResult expected)
        {
            var result = BatchId.Validate(token);
            Assert.Equal(expected, result);
        }
    }
}
