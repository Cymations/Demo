namespace ETL
{
    public interface ILoader<TData>
    {
        void Load(IEnumerable<TData> data);
    }
}