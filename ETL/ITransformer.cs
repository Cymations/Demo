namespace ETL
{
    public interface ITransformer<TInput, TOutput>
    {
        TOutput Transform(TInput input);
    }
}