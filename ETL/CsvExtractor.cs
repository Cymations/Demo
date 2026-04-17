using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;

namespace ETL
{
    /// <summary>
    /// Extracts rows from a CSV file, optionally reading the header.
    /// </summary>
    public class CsvExtractor : IExtractor<string, string[]>
    {
        /// <summary>
        /// Gets the header row if present, otherwise null.
        /// </summary>
        public string[]? Header { get; private set; }
        /// <summary>
        /// Extracts all rows from the specified CSV file, assuming no header.
        /// </summary>
        /// <param name="filePath">The path to the CSV file.</param>
        /// <returns>An enumerable of string arrays representing rows.</returns>
        /// <exception cref="ArgumentNullException">Thrown if filePath is null or empty.</exception>
        /// <exception cref="FileNotFoundException">Thrown if the file does not exist.</exception>
        public IEnumerable<string[]> Extract(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentNullException(nameof(filePath), "File path cannot be null or empty.");
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"File not found: {filePath}", filePath);
            return Extract(filePath, false);
        }

        /// <summary>
        /// Extracts all rows from the specified CSV file, optionally reading the header.
        /// </summary>
        /// <param name="filePath">The path to the CSV file.</param>
        /// <param name="hasHeader">Whether the CSV file contains a header row.</param>
        /// <returns>An enumerable of string arrays representing rows.</returns>
        /// <exception cref="ArgumentNullException">Thrown if filePath is null or empty.</exception>
        /// <exception cref="FileNotFoundException">Thrown if the file does not exist.</exception>
        /// <exception cref="IOException">Thrown if an I/O error occurs while reading the file.</exception>
        public IEnumerable<string[]> Extract(string filePath, bool hasHeader)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentNullException(nameof(filePath), "File path cannot be null or empty.");
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"File not found: {filePath}", filePath);

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = hasHeader,
                BadDataFound = null // Ignore bad data events, handle in transform
            };
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, config))
            {
                if (hasHeader && csv.Read())
                {
                    csv.ReadHeader();
                    Header = csv.HeaderRecord;
                }
                while (csv.Read())
                {
                    var row = new List<string>();
                    for (int i = 0; csv.TryGetField<string>(i, out var field); i++)
                    {
                        row.Add(field ?? string.Empty);
                    }
                    yield return row.ToArray();
                }
            }
        }
    }
}