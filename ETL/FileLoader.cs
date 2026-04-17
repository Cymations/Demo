using System.Collections.Generic;
using System.IO;

namespace ETL
{
    /// <summary>
    /// Loads lines of text to a file.
    /// </summary>
    public class FileLoader : ILoader<string>
    {
        private readonly string _outputPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileLoader"/> class.
        /// </summary>
        /// <param name="outputPath">The path to the output file.</param>
        public FileLoader(string outputPath)
        {
            _outputPath = outputPath;
        }

        /// <summary>
        /// Loads the specified lines of text to the output file.
        /// </summary>
        /// <param name="data">The lines of text to write.</param>
        public void Load(IEnumerable<string> data)
        {
            File.WriteAllLines(_outputPath, data);
        }
    }
}