
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;

namespace ETL
{
    public class CsvExtractor : IExtractor<string, string[]>
    {
        public string[]? Header { get; private set; }
        public IEnumerable<string[]> Extract(string filePath)
        {
            return Extract(filePath, false);
        }

        public IEnumerable<string[]> Extract(string filePath, bool hasHeader)
        {
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