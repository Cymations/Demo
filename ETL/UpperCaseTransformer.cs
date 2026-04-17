namespace ETL
{
    public class UpperCaseTransformer : ITransformer<string, string>
    {
        public string Transform(string input)
        {
            return input.ToUpperInvariant();
        }
    }
}