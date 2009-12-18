using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace DbSlim
{
  public class Insert
  {
        string connectionString;
        string tableName;
        bool identityInsert;

    public Insert(string connectionString, string tableName)
    {
      this.connectionString = connectionString;
      this.tableName = tableName;
    }

    public Insert(string connectionString, string tableName, string identyInsert)
    {
        this.identityInsert = Convert.ToBoolean(identyInsert);
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

    private IDbCommand GetInsertCommand(SqlConnection connection)
  {
      using (var selectCommand = new SqlCommand("select * from " + tableName, connection))
      {
          var adapter = new SqlDataAdapter(selectCommand);
          var builder = new SqlCommandBuilder(adapter);
          return builder.GetInsertCommand();
      }
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

            try
            {
                row[columnName] = Convert.ChangeType(value, type);
            }
            catch
            {
                throw new ApplicationException(string.Format("can't covert from {0} to {1} for column: {2}", value.GetType().Name, type.Name, columnName));
            }
        }
        table.Rows.Add(row);
      }
    }

    private void BulkLoadTableIntoDatabase(SqlConnection connection, DataTable table)
    {
      using (connection)
      {
        SetIdentyInsert(connection);

        InsertRows(connection, table);

        ResetIdentyInsert(connection);
      }
    }

    void InsertRows(SqlConnection connection, DataTable table)
    {
      if(table.Rows.Count > 0)
      {
          using (var selectCommand = new SqlCommand("select * from " + tableName, connection))
          {
              var adapter = new SqlDataAdapter(selectCommand);
              var builder = new SqlCommandBuilder(adapter);
              var columns = GetColumnNames(table);
              adapter.InsertCommand = builder.GetInsertCommand(true);
              adapter.InsertCommand.Parameters.AddWithValue("@SecurityId", 50);
              adapter.Update(table);
          }
      }
    }

    void ResetIdentyInsert(SqlConnection connection)
    {
      if (identityInsert)
          using (var command = connection.CreateCommand())
          {
              command.CommandText = string.Format("set identity_insert {0} off",tableName);
              command.ExecuteNonQuery();
          }
    }

    void SetIdentyInsert(SqlConnection connection)
    {
      if(identityInsert)
      using(var command = connection.CreateCommand())
      {
          command.CommandText = string.Format("set identity_insert {0} on", tableName);
          command.ExecuteNonQuery();
      }
    }

    List<string> GetColumnNames(DataTable table)
    {
      var columnNames = new List<string>();
      for (int i = 0; i < table.Columns.Count; i++)
      {
          columnNames.Add(table.Columns[i].ColumnName);
      }
      return columnNames;
    }
  }
}