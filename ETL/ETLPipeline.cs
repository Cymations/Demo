using System.Collections.Generic;
using System.Linq;

namespace ETL
{
    public class ETLPipeline<TSource, TInput, TOutput>
    {
        private readonly IExtractor<TSource, TInput> _extractor;
        private readonly ITransformer<TInput, TOutput> _transformer;
        private readonly ILoader<TOutput> _loader;

        public ETLPipeline(
            IExtractor<TSource, TInput> extractor,
            ITransformer<TInput, TOutput> transformer,
            ILoader<TOutput> loader)
        {
            _extractor = extractor;
            _transformer = transformer;
            _loader = loader;
        }

        public void Run(TSource source)
        {
            var extracted = _extractor.Extract(source);
            var transformed = extracted.Select(_transformer.Transform);
            _loader.Load(transformed);
        }
    }
}