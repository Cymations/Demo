namespace ETL
{
    /// <summary>
    /// Defines a method for transforming input data to output data.
    /// </summary>
    /// <typeparam name="TInput">The type of input data.</typeparam>
    /// <typeparam name="TOutput">The type of output data.</typeparam>
    public interface ITransformer<TInput, TOutput>
    {
        /// <summary>
        /// Transforms the specified input data to output data.
        /// </summary>
        /// <param name="input">The input data to transform.</param>
        /// <returns>The transformed output data.</returns>
        TOutput Transform(TInput input);
    }
}