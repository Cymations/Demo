
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;

namespace ETL
{
    public class CsvExtractor : IExtractor<string, string[]>
    {
        public IEnumerable<string[]> Extract(string filePath)
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = false,
                BadDataFound = null // Ignore bad data events, handle in transform
            };
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, config))
            {
                while (csv.Read())
                {
                    var row = new List<string>();
                    for (int i = 0; csv.TryGetField<string>(i, out var field); i++)
                    {
                        row.Add(field);
                    }
                    yield return row.ToArray();
                }
            }
        }
    }
}