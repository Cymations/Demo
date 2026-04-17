using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ETL
{
    public class CsvLoader : ILoader<CsvTransformResult>
    {
        private readonly string _outputPath;
        private readonly string _rejectPath;

        public CsvLoader(string outputPath, string rejectPath)
        {
            _outputPath = outputPath;
            _rejectPath = rejectPath;
        }

        public void Load(IEnumerable<CsvTransformResult> results)
        {
            var validRows = new List<string[]>();
            var invalidRows = new List<(int, string[], string[])>();
            foreach (var result in results)
            {
                validRows.AddRange(result.ValidRows);
                invalidRows.AddRange(result.InvalidRows);
            }
            // Write valid rows to output CSV
            using (var writer = new StreamWriter(_outputPath))
            {
                foreach (var row in validRows)
                {
                    writer.WriteLine(string.Join(",", row));
                }
            }
            // Write invalid rows to reject report
            using (var writer = new StreamWriter(_rejectPath))
            {
                foreach (var (rowNum, row, errors) in invalidRows)
                {
                    writer.WriteLine($"{rowNum},{string.Join(",", row)},{string.Join("|", errors)}");
                }
            }
        }
    }
}