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

        public void Run(string inputPath)
        {
            var extracted = _extractor.Extract(inputPath);
            var transformed = _transformer.Transform(extracted);
            _loader.Load(new[] { transformed });
        }
    }
}