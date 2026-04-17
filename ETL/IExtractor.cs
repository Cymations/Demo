namespace ETL
{
    /// <summary>
    /// Defines a method for extracting data from a source.
    /// </summary>
    /// <typeparam name="TSource">The type of the source to extract from.</typeparam>
    /// <typeparam name="TData">The type of data extracted.</typeparam>
    public interface IExtractor<TSource, TData>
    {
        /// <summary>
        /// Extracts data from the specified source.
        /// </summary>
        /// <param name="source">The source to extract data from.</param>
        /// <returns>An enumerable of extracted data.</returns>
        IEnumerable<TData> Extract(TSource source);
    }
}