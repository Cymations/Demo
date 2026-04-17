using System.Collections.Generic;

namespace ETL
{
    public class CsvPipeline
    {
        private readonly CsvExtractor _extractor;
        private readonly CsvTransformer _transformer;
        private readonly CsvLoader _loader;

        public CsvPipeline(CsvExtractor extractor, CsvTransformer transformer, CsvLoader loader)
        {
            _extractor = extractor;
            _transformer = transformer;
            _loader = loader;
        }

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