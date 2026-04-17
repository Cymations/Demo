using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace ETL
{
    /// <summary>
    /// Represents the result of validating a batch ID, including validity, normalized value, and error codes.
    /// </summary>
    public class BatchIdResult
    {
        /// <summary>
        /// Gets or sets a value indicating whether the batch ID is valid.
        /// </summary>
        public bool IsValid { get; set; }

        /// <summary>
        /// Gets or sets the normalized batch ID value, or null if invalid.
        /// </summary>
        public string? Normalized { get; set; }

        /// <summary>
        /// Gets or sets the error codes associated with validation.
        /// </summary>
        public string[] ErrorCodes { get; set; } = Array.Empty<string>();
    }

    /// <summary>
    /// Provides methods for validating and parsing batch ID strings.
    /// </summary>
    public static class BatchIdParser
    {
        private static readonly Regex BatchIdRegex = new Regex(@"^(\d{8})-(\d{4})$", RegexOptions.Compiled);

        /// <summary>
        /// Trims whitespace from the batch ID token.
        /// </summary>
        /// <param name="token">The batch ID token to normalize.</param>
        /// <returns>The trimmed batch ID token.</returns>
        public static string NormalizeToken(string token)
        {
            return token.Trim();
        }

        /// <summary>
        /// Validates the batch ID token for format, date, and sequence correctness.
        /// </summary>
        /// <param name="token">The batch ID token to validate.</param>
        /// <returns>A <see cref="BatchIdResult"/> containing validation results, normalized value, and error codes.</returns>
        public static BatchIdResult Validate(string token)
        {
            var errors = new System.Collections.Generic.List<string>();
            var normalized = NormalizeToken(token);
            // Split BatchId into date and sequence parts
            var parts = normalized.Split('-');
            if (parts.Length != 2)
            {
                errors.Add("FORMAT");
                return new BatchIdResult { IsValid = false, ErrorCodes = errors.ToArray() };
            }
            var datePart = parts[0];
            var seqPart = parts[1];
            // Validate date using DateTime.TryParseExact
            if (!DateTime.TryParseExact(datePart, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            {
                errors.Add("DATE");
            }
            // Validate sequence: must be 4 digits, 0001-9999
            if (seqPart.Length != 4)
            {
                errors.Add("FORMAT");
            }
            else if (!int.TryParse(seqPart, out int seq) || seq < 1 || seq > 9999)
            {
                errors.Add("SEQUENCE");
            }
            return new BatchIdResult
            {
                IsValid = errors.Count == 0,
                Normalized = normalized,
                ErrorCodes = errors.ToArray()
            };
        }

        /// <summary>
        /// Attempts to validate and normalize a batch ID token.
        /// </summary>
        /// <param name="token">The batch ID token to parse.</param>
        /// <param name="normalized">When this method returns, contains the normalized batch ID if valid; otherwise, null.</param>
        /// <returns>True if the batch ID is valid; otherwise, false.</returns>
        public static bool TryParse(string token, out string? normalized)
        {
            var result = Validate(token);
            normalized = result.IsValid ? result.Normalized : null;
            return result.IsValid;
        }
    }
}