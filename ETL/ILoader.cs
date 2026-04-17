namespace ETL
{
    /// <summary>
    /// Defines a method for loading data to a destination.
    /// </summary>
    /// <typeparam name="TData">The type of data to load.</typeparam>
    public interface ILoader<TData>
    {
        /// <summary>
        /// Loads the specified data to the destination.
        /// </summary>
        /// <param name="data">The data to load.</param>
        void Load(IEnumerable<TData> data);
    }
}