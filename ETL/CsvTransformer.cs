using System.Collections.Generic;

namespace ETL
{
    public class CsvTransformResult
    {
        public List<string[]> ValidRows { get; } = new();
        public List<(int RowNumber, string[] Row, string[] ErrorCodes)> InvalidRows { get; } = new();
        public string[]? Header { get; set; }
    }

    public class CsvTransformer : ITransformer<IEnumerable<string[]>, CsvTransformResult>
    {
        private readonly int _batchIdColumnIndex;
        public CsvTransformer(int batchIdColumnIndex)
        {
            _batchIdColumnIndex = batchIdColumnIndex;
        }

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
}