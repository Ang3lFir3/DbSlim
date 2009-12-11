using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace DbSlim
{
  public class Insert
  {
    private string connectionString;
    private string tableName;

    public Insert(string connectionString, string tableName)
    {
      this.connectionString = connectionString;
      this.tableName = tableName;
    }

    public List<object> DoTable(List<List<object>> fitTable)
    {
      if (fitTable.Count < 2)
        throw new ApplicationException("There needs to be at least one header row and a data row.");


      using (var connection = new SqlConnection(connectionString))
      {
        connection.Open();

        var table = GetTableSchema(fitTable, connection);

        LoadDataTableFromSlimTable(fitTable, table);

        BulkLoadTableIntoDatabase(connection, table);
      }

      return null;
    }

    private DataTable GetTableSchema(List<List<object>> fitTable, SqlConnection connection)
    {
      using (var selectCommand = new SqlCommand("select * from " + tableName, connection))
      {
        var adapter = new SqlDataAdapter(selectCommand);

        var table = new DataTable();

        adapter.FillSchema(table, SchemaType.Source);

        VerifyTableColumnsMatch(fitTable, table);

        return table;
      }
    }

    private void VerifyTableColumnsMatch(List<List<object>> slimTable, DataTable table)
    {
      for (int i = 0; i < slimTable[0].Count; i++)
      {
        var columnName = slimTable[0][i].ToString();

        if (!table.Columns.Contains(columnName))
        {
          string message = string.Format("Table {0} does not contain a column {1}",
              tableName, columnName);

          throw new ApplicationException(message);
        }
      }
    }

    private static void LoadDataTableFromSlimTable(List<List<object>> slimTable, DataTable table)
    {
      for (var i = 1; i < slimTable.Count; i++)
      {
        var row = table.NewRow();

        for (int j = 0; j < slimTable[0].Count; j++)
        {
          var columnName = slimTable[0][j].ToString();

          var value = slimTable[i][j];
          var type = table.Columns[columnName].DataType;

          Console.WriteLine("columnName {0} : {1} as {2}", columnName, value, type);


          row[columnName] = Convert.ChangeType(value, type);          
        }
        table.Rows.Add(row);
        Console.WriteLine("Loaded {0} of {1} Rows", table.Rows.Count, i);
      }
    }

    private void BulkLoadTableIntoDatabase(SqlConnection connection, DataTable table)
    {
      using (var bulkCopy = new SqlBulkCopy(connection))
      {
        bulkCopy.DestinationTableName = tableName;

        Console.WriteLine("Destination table: {0}", tableName);
        Console.WriteLine("Table Size: {0}", table.Rows.Count);

        bulkCopy.WriteToServer(table);
      }
    }
  }
}