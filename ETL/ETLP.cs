using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace ETL
{
    /// <summary>
    /// Handles the extraction of data from CSV files.
    /// </summary>
    public class Extract
    {
        /// <summary>
        /// Loads data from a CSV file into a list of dictionaries.
        /// </summary>
        /// <param name="filePath">The path to the CSV file.</param>
        /// <returns>A list of dictionaries representing the rows of the CSV file.</returns>
        /// <example>
        /// <code>
        /// var extract = new Extract();
        /// var data = extract.LoadCsv("data.csv");
        /// </code>
        /// </example>
        /// <example>
        /// <code>
        /// var extract = new Extract();
        /// var data = extract.LoadCsv("input.csv");
        /// </code>
        /// </example>
        /// <exception cref="ArgumentException">Thrown if the file path is null or empty.</exception>
        /// <exception cref="Exception">Thrown if the CSV file has less than two lines.</exception>

        public List<Dictionary<string, string>> LoadCsv(string filePath)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(filePath))
                    throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));

                var lines = File.ReadAllLines(filePath);
                if (lines.Length < 2)
                    throw new Exception("CSV file must have at least a header and one data row.");

                var headers = lines[0].Split(',');
                return lines.Skip(1)
                            .Select(line => line.Split(',')
                                                 .Zip(headers, (value, header) => new { header, value })
                                                 .ToDictionary(x => x.header, x => x.value))
                            .ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in LoadCsv: {ex.Message}");
                throw;
            }
        }
    }

    /// <summary>
    /// Handles data transformation, including cleaning and normalization.
    /// </summary>
    public class Transform
    {
        /// <summary>
        /// Cleans and normalizes the data.
        /// </summary>
        /// <param name="data">The data to be transformed.</param>
        /// <param name="numericColumns">The list of numeric columns to normalize.</param>
        /// <returns>The cleaned and normalized data.</returns>
        /// <example>
        /// <code>
        /// var transform = new Transform();
        /// var normalizedData = transform.CleanAndNormalize(data, new List<string> { "Column1" });
        /// </code>
        /// </example>
        /// <example>
        /// <code>
        /// var transform = new Transform();
        /// var cleanedData = transform.CleanAndNormalize(data, new List<string> { "Column2" });
        /// </code>
        /// </example>
        /// <exception cref="ArgumentException">Thrown if numeric columns are missing or invalid.</exception>

        public List<Dictionary<string, string>> CleanAndNormalize(List<Dictionary<string, string>> data, List<string> numericColumns)
        {
            try
            {
                if (data == null || !data.Any())
                    return new List<Dictionary<string, string>>();

                if (numericColumns == null || !numericColumns.Any())
                    throw new ArgumentException("Numeric columns cannot be null or empty.", nameof(numericColumns));

                var cleanedData = data.Where(row => row.All(kv => !string.IsNullOrWhiteSpace(kv.Value))).ToList();

                foreach (var column in numericColumns)
                {
                    var numericValues = cleanedData.Select(row => double.TryParse(row[column], out var value) ? value : (double?)null)
                                                   .Where(value => value.HasValue)
                                                   .Select(value => value!.Value)
                                                   .ToList();

                    if (!numericValues.Any())
                        continue;

                    var min = numericValues.Min();
                    var max = numericValues.Max();

                    foreach (var row in cleanedData)
                    {
                        if (double.TryParse(row[column], out var value))
                        {
                            row[column] = ((value - min) / (max - min)).ToString(CultureInfo.InvariantCulture);
                        }
                    }
                }

                return cleanedData;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in CleanAndNormalize: {ex.Message}");
                throw;
            }
        }
    }

    /// <summary>
    /// Handles loading data into a CSV file.
    /// </summary>
    public class Load
    {
        /// <summary>
        /// Saves data to a CSV file.
        /// </summary>
        /// <param name="filePath">The path to save the CSV file.</param>
        /// <param name="data">The data to save.</param>
        /// <exception cref="ArgumentException">Thrown if the file path is null or empty.</exception>
        /// <exception cref="Exception">Thrown if the data is empty.</exception>
        /// <example>
        /// <code>
        /// var load = new Load();
        /// load.SaveCsv("output.csv", data);
        /// </code>
        /// </example>
        /// <example>
        /// <code>
        /// var load = new Load();
        /// load.SaveCsv("results.csv", data);
        /// </code>
        /// </example>

        public void SaveCsv(string filePath, List<Dictionary<string, string>> data)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(filePath))
                    throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));

                if (data == null || !data.Any())
                    throw new Exception("No data to save.");

                var headers = data.First().Keys;
                var lines = new List<string> { string.Join(",", headers) };

                lines.AddRange(data.Select(row => string.Join(",", headers.Select(header => row[header]))));

                File.WriteAllLines(filePath, lines);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in SaveCsv: {ex.Message}");
                throw;
            }
        }
    }

    /// <summary>
    /// Orchestrates the ETL pipeline.
    /// </summary>
    public class Pipeline
    {
        private Extract _extract;
        private Transform _transform;
        private Load _load;

        /// <summary>
        /// Initializes a new instance of the <see cref="Pipeline"/> class.
        /// </summary>
        /// <remarks>
        /// This constructor initializes the Extract, Transform, and Load components of the ETL pipeline.
        /// </remarks>

        public Pipeline()
        {
            _extract = new Extract();
            _transform = new Transform();
            _load = new Load();
        }
        /// <summary>
        /// Runs the ETL pipeline.
        /// </summary>
        /// <param name="inputFilePath">The input CSV file path.</param>
        /// <param name="outputFilePath">The output CSV file path.</param>
        /// <param name="numericColumns">The list of numeric columns to normalize.</param>
        /// <example>
        /// <code>
        /// var pipeline = new Pipeline();
        /// pipeline.Run("input.csv", "output.csv", new List<string> { "Column1" });
        /// </code>
        /// </example>
        /// <example>
        /// <code>
        /// var pipeline = new Pipeline();
        /// pipeline.Run("data.csv", "results.csv", new List<string> { "Column2" });
        /// </code>
        /// </example>
        /// <exception cref="ArgumentException">Thrown if any file path is null or empty.</exception>
        /// <exception cref="Exception">Thrown if any step of the pipeline fails.</exception>
        public void Run(string inputFilePath, string outputFilePath, List<string> numericColumns)
        {
            try
            {
                Console.WriteLine("Starting ETL pipeline...");

                Console.WriteLine("Extracting data...");
                var data = _extract.LoadCsv(inputFilePath);

                Console.WriteLine("Transforming data...");
                data = _transform.CleanAndNormalize(data, numericColumns);

                Console.WriteLine("Loading data...");
                _load.SaveCsv(outputFilePath, data);

                Console.WriteLine("ETL pipeline completed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in Run: {ex.Message}");
                throw;
            }
        }
    }
}
