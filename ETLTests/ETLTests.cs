using System;
using System.Collections.Generic;
using ETL;
using Xunit;

namespace ETLTests
{
    public class TransformTests
    {
        public static IEnumerable<object[]> ValidateBatchIdTestData => new List<object[]>
        {
            new object[] { "20230101-0001", true, new List<string>() }, // Happy path
            new object[] { "20230101-9999", true, new List<string>() }, // Upper boundary
            new object[] { "20230101-0000", false, new List<string> { "Invalid sequence. Must be a 4-digit number between 0001 and 9999." }.OrderBy(e => e).ToList() }, // Sequence too low
            new object[] { "20230101-10000", false, new List<string> { "Invalid sequence. Must be a 4-digit number between 0001 and 9999." }.OrderBy(e => e).ToList() }, // Sequence too high
            new object[] { "20230101-ABCD", false, new List<string> { "Invalid sequence. Must be a 4-digit number between 0001 and 9999." }.OrderBy(e => e).ToList() }, // Non-numeric sequence
            new object[] { "20231301-0001", false, new List<string> { "Invalid date." }.OrderBy(e => e).ToList() }, // Invalid month
            new object[] { "20230132-0001", false, new List<string> { "Invalid date." }.OrderBy(e => e).ToList() }, // Invalid day
            new object[] { "20230101_0001", false, new List<string> { "Invalid format. Expected yyyyMMdd-SSSS." }.OrderBy(e => e).ToList() }, // Invalid format
        };

        [Theory]
        [MemberData(nameof(ValidateBatchIdTestData))]
        public void ValidateBatchId_ShouldValidateCorrectly(string batchId, bool expectedIsValid, List<string> expectedErrors)
        {
            // Arrange
            var transform = new Transform();

            // Act
            var (isValid, errors, debugInfo) = transform.ValidateBatchId(batchId);

            // Log debugging information
            Console.WriteLine(debugInfo);

            // Assert
            Assert.Equal(expectedIsValid, isValid);
            foreach (var expectedError in expectedErrors)
            {
                Assert.Contains(expectedError, errors);
            }
        }

        [Theory]
        [InlineData("20230101-0001", true, null)] // Correct format
        [InlineData("20231301-0001", false, "Invalid date.")] // Invalid month
        [InlineData("20230230-0001", false, "Invalid date.")] // Invalid day
        [InlineData("20230101-99999", false, "Invalid sequence. Must be a 4-digit number between 0001 and 9999.")] // Sequence too long
        [InlineData("InvalidBatchId", false, "Invalid format. Expected yyyyMMdd-SSSS.")] // Completely invalid format
        public void ValidateBatchId_ShouldHandleVariousCases(string batchId, bool expectedIsValid, string expectedError)
        {
            // Arrange
            var transform = new Transform();

            // Act
            var result = transform.ValidateBatchId(batchId);

            // Assert
            Assert.Equal(expectedIsValid, result.IsValid);
            if (!expectedIsValid && expectedError != null)
            {
                Assert.Contains(expectedError, result.Errors);
            }
        }

        public static IEnumerable<object[]> NormalizeBatchIdTestData => new List<object[]>
        {
            new object[] { "20230101-0001", "20230101-0001" }, // Already uppercase
        };

        [Theory]
        [MemberData(nameof(NormalizeBatchIdTestData))]
        public void NormalizeBatchId_ShouldNormalizeCorrectly(string batchId, string expectedNormalized)
        {
            // Arrange
            var transform = new Transform();

            // Act
            var normalized = transform.NormalizeBatchId(batchId);

            // Assert
            Assert.Equal(expectedNormalized, normalized);
        }
    }

    public class ExtractTests
    {
        [Fact]
        public void LoadCsv_ShouldThrowException_WhenFilePathIsNullOrEmpty()
        {
            // Arrange
            var extract = new Extract();

            // Act & Assert
            Assert.Throws<ArgumentException>(() => extract.LoadCsv(string.Empty));
        }

        [Fact]
        public void LoadCsv_ShouldThrowException_WhenFileHasInsufficientRows()
        {
            // Arrange
            var extract = new Extract();
            var filePath = "test.csv";
            System.IO.File.WriteAllLines(filePath, new[] { "Header1,Header2" });

            try
            {
                // Act & Assert
                Assert.Throws<Exception>(() => extract.LoadCsv(filePath));
            }
            finally
            {
                System.IO.File.Delete(filePath);
            }
        }

        [Fact]
        public void LoadCsv_ShouldDisplayDataCorrectly()
        {
            // Arrange
            var extract = new Extract();
            var filePath = "test.csv";
            System.IO.File.WriteAllLines(filePath, new[]
            {
                "Header1,Header2",
                "Value1,Value2"
            });

            try
            {
                // Act
                var result = extract.LoadCsv(filePath);

                // Assert
                Assert.Single(result);
                Assert.Equal("Value1", result[0]["Header1"]);
                Assert.Equal("Value2", result[0]["Header2"]);
            }
            finally
            {
                System.IO.File.Delete(filePath);
            }
        }

        [Fact]
        public void LoadCsv_ShouldHandleMultipleColumnsPerRow()
        {
            // Arrange
            var extract = new Extract();
            var filePath = "test_multiple_columns.csv";
            System.IO.File.WriteAllLines(filePath, new[]
            {
                "Column1,Column2,Column3",
                "Value1,Value2,Value3",
                "Data1,Data2,Data3"
            });

            try
            {
                // Act
                var result = extract.LoadCsv(filePath);

                // Assert
                Assert.Equal(2, result.Count); // Two rows of data
                Assert.Equal("Value1", result[0]["Column1"]);
                Assert.Equal("Value2", result[0]["Column2"]);
                Assert.Equal("Value3", result[0]["Column3"]);
                Assert.Equal("Data1", result[1]["Column1"]);
                Assert.Equal("Data2", result[1]["Column2"]);
                Assert.Equal("Data3", result[1]["Column3"]);
            }
            finally
            {
                System.IO.File.Delete(filePath);
            }
        }

        [Fact]
        public void LoadCsv_ShouldValidateFirstColumnAsBatchId()
        {
            // Arrange
            var extract = new Extract();
            var transform = new Transform();
            var filePath = "test_validate_batchid.csv";
            System.IO.File.WriteAllLines(filePath, new[]
            {
                "BatchId,Column2,Column3",
                "20230101-0001,Value2,Value3",
                "InvalidBatchId,Data2,Data3"
            });

            try
            {
                // Act
                var result = extract.LoadCsv(filePath);
                var validationResults = result.Select(row => transform.ValidateBatchId(row["BatchId"])).ToList();

                // Assert
                Assert.True(validationResults[0].IsValid); // First row has a valid BatchId
                Assert.False(validationResults[1].IsValid); // Second row has an invalid BatchId
                Assert.Contains("Invalid format. Expected yyyyMMdd-SSSS.", validationResults[1].Errors);
            }
            finally
            {
                System.IO.File.Delete(filePath);
            }
        }
    }
}