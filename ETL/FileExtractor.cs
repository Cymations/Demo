using System.Collections.Generic;
using System.IO;

namespace ETL
{
    /// <summary>
    /// Extracts lines from a text file.
    /// </summary>
    public class FileExtractor : IExtractor<string, string>
    {
        /// <summary>
        /// Extracts all lines from the specified file.
        /// </summary>
        /// <param name="filePath">The path to the file.</param>
        /// <returns>An enumerable of lines from the file.</returns>
        public IEnumerable<string> Extract(string filePath)
        {
            return File.ReadLines(filePath);
        }
    }
}