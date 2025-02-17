using DatabaseOperation.Model;

namespace DatabaseOperation.Interfaces
{
    public interface IDatabase
    {
        int Execute(TableData properties);
        IEnumerable<int> Execute(IEnumerable<TableData> listOfTableProperties);
    }
}