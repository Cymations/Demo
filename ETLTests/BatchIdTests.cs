using System;
using ETL;
using Xunit;

namespace ETLTests
// Suppress nullable warnings for test null argument cases
#pragma warning disable CS8604
{
    public class BatchIdTests
    {
        [Theory]
        [InlineData("20240417-0001", 2024, 4, 17, 1)]
        [InlineData("19991231-1234", 1999, 12, 31, 1234)]
        public void TryParse_ValidToken_ReturnsTrueAndCorrectValues_ShouldReturnTrueAndCorrectBatchId(string token, int year, int month, int day, int seq)
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
        public void TryParse_InvalidToken_ReturnsFalse_ShouldReturnFalseAndNullBatchId(string? token)
        {
            var result = BatchId.TryParse(token, out var batchId);
            Assert.False(result);
            Assert.Null(batchId);
        }

        [Theory]
        [InlineData("20240417-0001 ")]
        [InlineData(" 20240417-0001")]
        [InlineData("   ")]
        [InlineData("20240417_0001")]
        [InlineData("2024041-70001")]
        public void TryParse_EdgeCases_ReturnsFalse_ShouldReturnFalseAndNullBatchId(string? token)
        {
            var result = BatchId.TryParse(token, out var batchId);
            Assert.False(result);
            Assert.Null(batchId);
        }

        // ...existing code...
    }
}
