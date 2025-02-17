namespace DatabaseOperation.Model
{
    public class TableData
    {
        public List<TableProperty> properties = [];
        public EnumOperation enumOperation;
        public string schema;
        public string TableName;
        public List<List<KeyValuePair<string, dynamic>>> Data = new();
        public class TableRecords
        {
            public string Name { get; set; }
            public string Value { get; set; }
        }
    }
}
