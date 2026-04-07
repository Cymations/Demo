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

        public static IEnumerable<object[]> NormalizeBatchIdTestData => new List<object[]>
        {
            new object[] { "20230101-0001", "20230101-0001" }, // Already uppercase
            new object[] { "20230101-0001".ToLower(), "20230101-0001" }, // Lowercase input
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
        public void LoadCsv_ShouldLoadDataCorrectly()
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
    }
}