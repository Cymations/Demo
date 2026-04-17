using System;
using System.Globalization;

namespace ETL
{
    public class BatchId
    {
        public DateTime Date { get; }
        public int Sequence { get; }

        private BatchId(DateTime date, int sequence)
        {
            Date = date;
            Sequence = sequence;
        }

        public static string NormalizeToken(string token)
        {
            return token?.Trim();
        }

        public static bool TryParse(string token, out BatchId batchId)
        {
            batchId = null;
            token = NormalizeToken(token);
            if (string.IsNullOrEmpty(token) || token.Length != 13 || token[8] != '-')
                return false;
            var datePart = token.Substring(0, 8);
            var seqPart = token.Substring(9, 4);
            if (!DateTime.TryParseExact(datePart, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
                return false;
            if (!int.TryParse(seqPart, out var seq) || seq < 1 || seq > 9999)
                return false;
            batchId = new BatchId(date, seq);
            return true;
        }

        public static BatchIdParseResult Validate(string token)
        {
            if (string.IsNullOrWhiteSpace(token))
                return BatchIdParseResult.Missing;
            if (token.Length != 13 || token[8] != '-')
                return BatchIdParseResult.InvalidFormat;
            var datePart = token.Substring(0, 8);
            var seqPart = token.Substring(9, 4);
            if (!DateTime.TryParseExact(datePart, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
                return BatchIdParseResult.InvalidDate;
            if (!int.TryParse(seqPart, out var seq) || seq < 1 || seq > 9999)
                return BatchIdParseResult.InvalidSequence;
            return BatchIdParseResult.Valid;
        }
    }

    public enum BatchIdParseResult
    {
        Valid,
        Missing,
        InvalidFormat,
        InvalidDate,
        InvalidSequence
    }
}
