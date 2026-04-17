using System.Collections.Generic;
using System.IO;

namespace ETL
{
    public class FileExtractor : IExtractor<string, string>
    {
        public IEnumerable<string> Extract(string filePath)
        {
            return File.ReadLines(filePath);
        }
    }
}