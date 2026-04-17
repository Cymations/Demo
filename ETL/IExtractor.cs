namespace ETL
{
    public interface IExtractor<TSource, TData>
    {
        IEnumerable<TData> Extract(TSource source);
    }
}