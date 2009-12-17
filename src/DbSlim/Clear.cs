using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace DbSlim
{
    public class Clear
    {
        public List<object> DoTable(List<List<object>> fitTable)
        {
            foreach (var row in fitTable)
            {
                using(var connection = new SqlConnection(row[0].ToString()))
                {
                    connection.Open();
                    DeleteRows(connection, row[1].ToString());
                }
            }

            return null;
        }

        void DeleteRows(SqlConnection connection, string tableNames )
        {
            var seperators = new []{",",";"};
            var names = tableNames.Split(seperators,StringSplitOptions.RemoveEmptyEntries);
            foreach(var table in names){
                ClearTable(connection, table);  
            }
        }

        void ClearTable(SqlConnection connection, string table)
        {
            using(var command = connection.CreateCommand())
            {

                command.CommandText = string.Format("Delete from {0}", table);
                command.ExecuteNonQuery();
            }
        }
    }
}