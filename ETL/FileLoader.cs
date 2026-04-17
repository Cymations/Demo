using System.Collections.Generic;
using System.IO;

namespace ETL
{
    public class FileLoader : ILoader<string>
    {
        private readonly string _outputPath;

        public FileLoader(string outputPath)
        {
            _outputPath = outputPath;
        }

        public void Load(IEnumerable<string> data)
        {
            File.WriteAllLines(_outputPath, data);
        }
    }
}