using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace ETL
{
    public class BatchIdResult
    {
        public bool IsValid { get; set; }
        public string? Normalized { get; set; }
        public string[] ErrorCodes { get; set; } = Array.Empty<string>();
    }

    public static class BatchIdParser
    {
        private static readonly Regex BatchIdRegex = new Regex(@"^(\d{8})-(\d{4})$", RegexOptions.Compiled);

        public static string NormalizeToken(string token)
        {
            return token.Trim();
        }

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
            if (seqPart.Length != 4 || !int.TryParse(seqPart, out int seq) || seq < 1 || seq > 9999)
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

        public static bool TryParse(string token, out string? normalized)
        {
            var result = Validate(token);
            normalized = result.IsValid ? result.Normalized : null;
            return result.IsValid;
        }
    }
}