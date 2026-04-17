using System.Collections.Generic;

namespace ETL
{
    /// <summary>
    /// Represents the result of transforming CSV data, including valid and invalid rows.
    /// </summary>
    public class CsvTransformResult
    {
        public List<string[]> ValidRows { get; } = new();
        public List<(int RowNumber, string[] Row, string[] ErrorCodes)> InvalidRows { get; } = new();
        public string[]? Header { get; set; }
    }

    /// <summary>
    /// Transforms CSV rows by validating batch IDs and separating valid/invalid rows.
    /// </summary>
    public class CsvTransformer : ITransformer<IEnumerable<string[]>, CsvTransformResult>
    {
        private readonly int _batchIdColumnIndex;
        /// <summary>
        /// Initializes a new instance of the <see cref="CsvTransformer"/> class.
        /// </summary>
        /// <param name="batchIdColumnIndex">The index of the batch ID column.</param>
        public CsvTransformer(int batchIdColumnIndex)
        {
            _batchIdColumnIndex = batchIdColumnIndex;
        }

        /// <summary>
        /// Transforms the input CSV rows, validating batch IDs and separating valid and invalid rows.
        /// </summary>
        /// <param name="input">The input CSV rows.</param>
        /// <returns>A <see cref="CsvTransformResult"/> containing valid and invalid rows.</returns>
        public CsvTransformResult Transform(IEnumerable<string[]> input)
        {
            var result = new CsvTransformResult();
            int rowNum = 1;
            foreach (var row in input)
            {
                if (row.Length <= _batchIdColumnIndex)
                {
                    result.InvalidRows.Add((rowNum, row, new[] { "MISSING_BATCHID" }));
                }
                else
                {
                    var batchId = row[_batchIdColumnIndex];
                    var validation = BatchIdParser.Validate(batchId);
                    if (validation.IsValid)
                    {
                        var newRow = (string[])row.Clone();
                        newRow[_batchIdColumnIndex] = validation.Normalized!;
                        result.ValidRows.Add(newRow);
                    }
                    else
                    {
                        result.InvalidRows.Add((rowNum, row, validation.ErrorCodes));
                    }
                }
                rowNum++;
            }
            return result;
        }
    }

    /// <summary>
    /// Provides validation for batch IDs.
    /// </summary>
    public static class BatchIdParser
    {
        public class ValidationResult
        {
            public bool IsValid { get; set; }
            public string? Normalized { get; set; }
            public string[] ErrorCodes { get; set; } = new string[0];
        }

        /// <summary>
        /// Validates the batch ID.
        /// </summary>
        /// <param name="batchId">The batch ID to validate.</param>
        /// <returns>A ValidationResult indicating if the batch ID is valid, the normalized value, and any error codes.</returns>
        public static ValidationResult Validate(string batchId)
        {
            if (string.IsNullOrWhiteSpace(batchId))
            {
                return new ValidationResult
                {
                    IsValid = false,
                    ErrorCodes = new[] { "EMPTY_BATCHID" }
                };
            }
            // Example normalization: trim and uppercase
            var normalized = batchId.Trim().ToUpperInvariant();
            // Example validation: batchId must be alphanumeric and at least 5 chars
            if (normalized.Length < 5 || !System.Text.RegularExpressions.Regex.IsMatch(normalized, @"^[A-Z0-9]+$"))
            {
                return new ValidationResult
                {
                    IsValid = false,
                    ErrorCodes = new[] { "INVALID_FORMAT" }
                };
            }
            return new ValidationResult
            {
                IsValid = true,
                Normalized = normalized
            };
        }
    }
}