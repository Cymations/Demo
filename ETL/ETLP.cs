using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace ETL
{
    public class Extract
    {
        public List<Dictionary<string, string>> LoadCsv(string filePath)
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

        public List<Dictionary<string, string>> LoadJSON(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
            {
                throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
            }

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("The specified file does not exist.", filePath);
            }

            try
            {
                var jsonContent = File.ReadAllText(filePath);
                var data = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(jsonContent);

                if (data == null)
                {
                    throw new InvalidOperationException("Failed to parse JSON data.");
                }

                // Convert object values to string
                return data.Select(row => row.ToDictionary(
                    kv => kv.Key,
                    kv => kv.Value?.ToString() ?? string.Empty
                )).ToList();
            }
            catch (JsonException ex)
            {
                throw new InvalidOperationException("Error parsing JSON file.", ex);
            }
        }
    }

    public class Transform
    {
        public (List<Dictionary<string, string>> ValidRows, List<string> RejectReport) ProcessBatchId(
            List<Dictionary<string, string>> data, string batchIdColumn)
        {
            var validRows = new List<Dictionary<string, string>>();
            var rejectReport = new List<string>();

            foreach (var (row, index) in data.Select((row, index) => (row, index + 1)))
            {
                if (!row.TryGetValue(batchIdColumn, out var batchId))
                {
                    rejectReport.Add($"Row {index}: Missing {batchIdColumn}");
                    continue;
                }

                var validationResult = ValidateBatchId(batchId);
                if (!validationResult.IsValid)
                {
                    rejectReport.Add($"Row {index}: {string.Join(", ", validationResult.Errors)}");
                    continue;
                }

                row[batchIdColumn] = NormalizeBatchId(batchId);
                validRows.Add(row);
            }

            return (validRows, rejectReport);
        }

        /// <summary>
        /// Validates a batch ID for correct format, date, and sequence.
        /// </summary>
        /// <param name="batchId">The batch ID to validate. Expected format: yyyyMMdd-SSSS.</param>
        /// <returns>
        /// A tuple containing:
        /// <list type="bullet">
        /// <item><description><c>IsValid</c>: <see langword="true"/> if the batch ID is valid; otherwise, <see langword="false"/>.</description></item>
        /// <item><description><c>Errors</c>: A list of error messages explaining validation failures.</description></item>
        /// <item><description><c>DebugInfo</c>: Debugging information for logging purposes.</description></item>
        /// </list>
        /// </returns>
        /// <remarks>
        /// This method checks the following:
        /// <list type="bullet">
        /// <item><description>Format: The batch ID must be 13 characters long and include a hyphen at position 9.</description></item>
        /// <item><description>Date: The first 8 characters must represent a valid date in yyyyMMdd format.</description></item>
        /// <item><description>Sequence: The last 4 characters must be a number between 0001 and 9999.</description></item>
        /// </list>
        /// </remarks>
        /// <example>
        /// <code>
        /// var transform = new Transform();
        /// var result = transform.ValidateBatchId("20230101-0001");
        /// Console.WriteLine(result.IsValid); // Output: true
        /// </code>
        /// <code>
        /// var transform = new Transform();
        /// var result = transform.ValidateBatchId("20231301-0000");
        /// Console.WriteLine(string.Join(", ", result.Errors)); // Output: Invalid date., Invalid sequence. Must be a 4-digit number between 0001 and 9999.
        /// </code>
        /// </example>
        /// <errors>
        /// <list type="bullet">
        /// <item><description><c>Invalid format. Expected yyyyMMdd-SSSS.</c>: The batch ID does not match the required format.</description></item>
        /// <item><description><c>Invalid date.</c>: The date portion of the batch ID is not a valid calendar date.</description></item>
        /// <item><description><c>Invalid sequence. Must be a 4-digit number between 0001 and 9999.</c>: The sequence portion is not within the valid range.</description></item>
        /// </list>
        /// </errors>
        public (bool IsValid, List<string> Errors, string DebugInfo) ValidateBatchId(string batchId)
        {
            var errors = new List<string>();

            if (batchId.Length != 13 || batchId[8] != '-')
                errors.Add("Invalid format. Expected yyyyMMdd-SSSS.");

            if (!DateTime.TryParseExact(batchId.Substring(0, 8), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
                errors.Add("Invalid date.");

            if (!int.TryParse(batchId.Substring(9), out var sequence) || sequence < 1 || sequence > 9999)
                errors.Add("Invalid sequence. Must be a 4-digit number between 0001 and 9999.");

            var debugInfo = $"Debugging ValidateBatchId: Errors = {string.Join(", ", errors)}";

            return (errors.Count == 0, errors.OrderBy(e => e).ToList(), debugInfo);
        }

        public string NormalizeBatchId(string batchId)
        {
            return batchId.ToUpperInvariant();
        }

        public List<Dictionary<string, object>> TransformData(List<Dictionary<string, object>> data)
        {
            return data.Select(row =>
            {
                var transformedRow = new Dictionary<string, object>(row);
                if (row.ContainsKey("BatchId"))
                {
                    var batchIdObj = row["BatchId"];
                    var batchIdStr = batchIdObj?.ToString() ?? string.Empty;
                    transformedRow["BatchId"] = batchIdStr.ToUpper();
                }
                return transformedRow;
            }).ToList();
        }

        public List<Dictionary<string, string>> TransformData(List<Dictionary<string, string>> data)
        {
            return data.Select(row =>
            {
                var transformedRow = new Dictionary<string, string>(row);
                if (row.TryGetValue("BatchId", out var batchId) && !string.IsNullOrEmpty(batchId))
                {
                    transformedRow["BatchId"] = batchId.ToUpper();
                }
                return transformedRow;
            }).ToList();
        }
    }

    public class Load
    {
        public void SaveCsv(string filePath, List<Dictionary<string, string>> data)
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

        public void SaveRejectReport(string filePath, List<string> rejectReport)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));

            File.WriteAllLines(filePath, rejectReport);
        }

        public void LoadData(List<Dictionary<string, string>> data)
        {
            foreach (var row in data)
            {
                Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}: {kv.Value}")));
            }
        }
    }

    public class Pipeline
    {
        private readonly Extract _extract;
        private readonly Transform _transform;
        private readonly Load _load;

        public Pipeline()
        {
            _extract = new Extract();
            _transform = new Transform();
            _load = new Load();
        }

        public void Run(string inputFilePath, string outputFilePath, string rejectFilePath, string batchIdColumn)
        {
            Console.WriteLine("Starting ETL pipeline...");

            Console.WriteLine("Extracting data...");
            var data = _extract.LoadCsv(inputFilePath);

            Console.WriteLine("Transforming data...");
            var (validRows, rejectReport) = _transform.ProcessBatchId(data, batchIdColumn);

            Console.WriteLine("Loading valid data...");
            _load.SaveCsv(outputFilePath, validRows);

            Console.WriteLine("Saving reject report...");
            _load.SaveRejectReport(rejectFilePath, rejectReport);

            Console.WriteLine("ETL pipeline completed.");
        }

        public void Run(string csvFilePath, string jsonFilePath)
        {
            var extract = new Extract();
            var transform = new Transform();
            var load = new Load();

            // Load CSV data
            var csvData = extract.LoadCsv(csvFilePath);

            // Load JSON data
            var jsonData = extract.LoadJSON(jsonFilePath);

            // Transform and load data
            var transformedCsvData = transform.TransformData(csvData);
            var transformedJsonData = transform.TransformData(jsonData);

            load.LoadData(transformedCsvData);
            load.LoadData(transformedJsonData);
        }
    }
}