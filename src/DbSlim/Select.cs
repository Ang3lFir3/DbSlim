using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace DbSlim
{
  public class Select
  {
    readonly string connectionString;
    readonly string query;

    public Select(string connectionString, string query)
    {
      this.connectionString = connectionString;
      this.query = query;
    }

    public List<object> Query()
    {
      var result = new List<object>();

      using(var connection = new SqlConnection(connectionString))
      {
        connection.Open();

        var command = MakeCommand(connection, query);
        var table = LoadTable(command);

        result =  CreateResults(table);
        
      } 

      return result;
    }

    DataTable LoadTable(IDbCommand command)
    {
      var table = new DataTable();
        table.Load(command.ExecuteReader());
      return table;
    }

    IDbCommand MakeCommand(SqlConnection connection, string query)
    {
      var command = connection.CreateCommand();
      command.CommandText = query;
      return command;
    }

    List<object> CreateResults(DataTable table)
    {
      var columnNames = GetColumnNames(table);
      var result = new List<object>();

      for (int i = 0; i < table.Rows.Count; i++)
      {
        AddFieldValuePairs(result, columnNames, table.Rows[i]);
      }

      return result; 
    }

    void AddFieldValuePairs(List<object> objects, List<string> names, DataRow row)
    {
      var rowResult = new List<object>();
      foreach (var name in names)
      {
        var fieldValuePairs = new List<object> {name, row[name]};

        rowResult.Add(fieldValuePairs);
      }
      objects.Add(rowResult);

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