namespace ETL
{
    public class SimpleTransformer : ITransformer<string, string>
    {
        public string Transform(string extracted)
        {
            // Example transformation: convert to uppercase
            return extracted.ToUpperInvariant();
        }
    }
}
