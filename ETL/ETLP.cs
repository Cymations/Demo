using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

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
    }
}