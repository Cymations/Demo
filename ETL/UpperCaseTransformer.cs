namespace ETL
{
    /// <summary>
    /// Transforms a string to upper case using invariant culture.
    /// </summary>
    public class UpperCaseTransformer : ITransformer<string, string>
    {
        /// <summary>
        /// Transforms the input string to upper case using invariant culture.
        /// </summary>
        /// <param name="input">The input string to transform.</param>
        /// <returns>The upper-cased string.</returns>
        public string Transform(string input)
        {
            return input.ToUpperInvariant();
        }
    }
}