using System.Collections.Generic;

namespace ETL
{
    /// <summary>
    /// Orchestrates the CSV ETL process using extractor, transformer, and loader.
    /// </summary>
    public class CsvPipeline
    {
        private readonly CsvExtractor _extractor;
        private readonly CsvTransformer _transformer;
        private readonly CsvLoader _loader;

        /// <summary>
        /// Initializes a new instance of the <see cref="CsvPipeline"/> class.
        /// </summary>
        /// <param name="extractor">The CSV extractor.</param>
        /// <param name="transformer">The CSV transformer.</param>
        /// <param name="loader">The CSV loader.</param>
        public CsvPipeline(CsvExtractor extractor, CsvTransformer transformer, CsvLoader loader)
        {
            _extractor = extractor;
            _transformer = transformer;
            _loader = loader;
        }

        /// <summary>
        /// Runs the CSV ETL pipeline on the specified input file.
        /// </summary>
        /// <param name="inputPath">The path to the input CSV file.</param>
        /// <param name="hasHeader">Whether the CSV file contains a header row.</param>
        public void Run(string inputPath, bool hasHeader = false)
        {
            var extracted = _extractor.Extract(inputPath, hasHeader);
            var transformed = _transformer.Transform(extracted);
            // propagate header if present
            if (_extractor.Header != null)
                transformed.Header = _extractor.Header;
            _loader.Load(new[] { transformed });
        }
    }
}