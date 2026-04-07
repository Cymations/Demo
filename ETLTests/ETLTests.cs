using System;
using System.Collections.Generic;
using System.IO;
using ETL;
using Xunit;

namespace ETL.Tests
{
    public class ETLTests
    {
        [Fact]
        public void LoadCsv_ValidInput_ReturnsData()
        {
            // Arrange
            var extract = new Extract();
            var filePath = "test.csv";
            File.WriteAllLines(filePath, new[] { "Column1,Column2", "Value1,Value2" });

            // Act
            var data = extract.LoadCsv(filePath);

            // Assert
            Assert.NotNull(data);
            Assert.Single(data);
            Assert.Equal("Value1", data[0]["Column1"]);

            // Cleanup
            File.Delete(filePath);
        }

        [Fact]
        public void LoadCsv_InvalidInput_ThrowsException()
        {
            // Arrange
            var extract = new Extract();
            var filePath = "test_invalid.csv";
            File.WriteAllLines(filePath, new[] { "Column1,Column2" });

            // Act & Assert
            var exception = Assert.Throws<Exception>(() => extract.LoadCsv(filePath));
            Assert.Equal("CSV file must have at least a header and one data row.", exception.Message);

            // Cleanup
            File.Delete(filePath);
        }

        [Fact]
        public void CleanAndNormalize_ValidData_NormalizesNumericColumns()
        {
            // Arrange
            var transform = new Transform();
            var data = new List<Dictionary<string, string>>
            {
                new Dictionary<string, string> { { "Column1", "1" }, { "Column2", "2" } },
                new Dictionary<string, string> { { "Column1", "3" }, { "Column2", "4" } }
            };
            var numericColumns = new List<string> { "Column1" };

            // Act
            var result = transform.CleanAndNormalize(data, numericColumns);

            // Assert
            Assert.Equal("0", result[0]["Column1"]);
            Assert.Equal("1", result[1]["Column1"]);
        }

        [Fact]
        public void SaveCsv_ValidData_SavesFile()
        {
            // Arrange
            var load = new Load();
            var filePath = "output.csv";
            var data = new List<Dictionary<string, string>>
            {
                new Dictionary<string, string> { { "Column1", "Value1" }, { "Column2", "Value2" } }
            };

            // Act
            load.SaveCsv(filePath, data);

            // Assert
            Assert.True(File.Exists(filePath));
            var lines = File.ReadAllLines(filePath);
            Assert.Equal("Column1,Column2", lines[0]);
            Assert.Equal("Value1,Value2", lines[1]);

            // Cleanup
            File.Delete(filePath);
        }

        [Fact]
        public void SaveCsv_EmptyData_ThrowsException()
        {
            // Arrange
            var load = new Load();
            var filePath = "output_empty.csv";
            var data = new List<Dictionary<string, string>>();

            // Act & Assert
            var exception = Assert.Throws<Exception>(() => load.SaveCsv(filePath, data));
            Assert.Equal("No data to save.", exception.Message);
        }

        [Fact]
        public void Run_ValidPipeline_ExecutesSuccessfully()
        {
            // Arrange
            var pipeline = new Pipeline();
            var inputFilePath = "input.csv";
            var outputFilePath = "output_pipeline.csv";
            File.WriteAllLines(inputFilePath, new[] { "Column1,Column2", "1,2", "3,4" });
            var numericColumns = new List<string> { "Column1" };

            // Act
            pipeline.Run(inputFilePath, outputFilePath, numericColumns);

            // Assert
            Assert.True(File.Exists(outputFilePath));
            var lines = File.ReadAllLines(outputFilePath);
            Assert.Equal("Column1,Column2", lines[0]);
            Assert.Equal("0,2", lines[1]);
            Assert.Equal("1,4", lines[2]);

            // Cleanup
            File.Delete(inputFilePath);
            File.Delete(outputFilePath);
        }
    }
}
