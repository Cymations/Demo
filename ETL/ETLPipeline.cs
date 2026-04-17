using System.Collections.Generic;
using System.Linq;

namespace ETL
{
    /// <summary>
    /// Represents a generic ETL (Extract, Transform, Load) pipeline.
    /// </summary>
    /// <typeparam name="TSource">The source type for extraction.</typeparam>
    /// <typeparam name="TInput">The intermediate type after extraction.</typeparam>
    /// <typeparam name="TOutput">The output type after transformation.</typeparam>
    public class ETLPipeline<TSource, TInput, TOutput>
    {
        private readonly IExtractor<TSource, TInput> _extractor;
        private readonly ITransformer<TInput, TOutput> _transformer;
        private readonly ILoader<TOutput> _loader;

        /// <summary>
        /// Initializes a new instance of the <see cref="ETLPipeline{TSource, TInput, TOutput}"/> class.
        /// </summary>
        /// <param name="extractor">The extractor component.</param>
        /// <param name="transformer">The transformer component.</param>
        /// <param name="loader">The loader component.</param>
        public ETLPipeline(
            IExtractor<TSource, TInput> extractor,
            ITransformer<TInput, TOutput> transformer,
            ILoader<TOutput> loader)
        {
            _extractor = extractor;
            _transformer = transformer;
            _loader = loader;
        }

        /// <summary>
        /// Runs the ETL pipeline: extracts, transforms, and loads data from the given source.
        /// </summary>
        /// <param name="source">The source to extract data from.</param>
        public void Run(TSource source)
        {
            var extracted = _extractor.Extract(source);
            var transformed = extracted.Select(_transformer.Transform);
            _loader.Load(transformed);
        }
    }
}