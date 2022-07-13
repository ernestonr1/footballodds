using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlOperations
{
    public class SqlInserts
    {
        public string FilePath { get; set; }
        public string DatabaseConnectionString { get; set; }
        public bool HasReferee { get; set; }
        public bool IsEnglish { get; set; }

        public SqlInserts(string filePath, string databaseConnectionString, bool hasReferee = true, bool isEnglish = true)
        {
            FilePath = filePath;
            DatabaseConnectionString = databaseConnectionString;
            HasReferee = hasReferee;
            IsEnglish = isEnglish;
        }

        public void PushTeamsToDatabase()
        {
            List<string> firstRow = new List<string>();
            List<string> result = new List<string>();

            using (SqlConnection con = new SqlConnection($"{DatabaseConnectionString}"))
            {
                con.Open();
                using (StreamReader reader = new StreamReader($"{FilePath}"))
                {
                    int counter = 0;
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        var values = line.Split(';', ',');
                        if (counter > 0)
                        {
                            firstRow.Add(values[2].ToString());
                            firstRow.Add(values[3].ToString());
                        }
                        counter++;
                    }
                    //saving to a distinct list to prevent saving duplicated records
                    result = firstRow.Distinct().ToList();
                }

                //not much to say, first we check if the record is present in the database, if it's we continue the loop and print a message,
                //otherwise we push it to the database with a message.
                foreach (var item in result)
                {
                    var query = $"Select Count(*) from Teams where TeamName = ('{item}')";

                    var insert = $"insert into Teams(TeamName) values('{item}')";
                    using (var command = new SqlCommand(query, con))
                    {
                        int rowExist = (int)command.ExecuteScalar();
                        if (rowExist > 0)
                        {
                            Console.WriteLine("Record already exists!");
                            continue;
                        }
                        else if (rowExist == 0)
                        {
                            using (var myInsert = new SqlCommand(insert, con))
                            {
                                myInsert.ExecuteNonQuery();
                                Console.WriteLine("Record Pushed to database!");
                            }
                        }
                    }
                }
                con.Close();
            }
        }

        public void PushToCountries()
        {
            List<int> teamIds = new List<int>();
            string countryName = "";
            using (SqlConnection con = new SqlConnection($"{DatabaseConnectionString}"))
            {
                con.Open();
                var selectTeamsId = "Select Id from Teams";
                using (var command = new SqlCommand(selectTeamsId, con))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            teamIds.Add(reader.GetInt32(0));
                        }
                        reader.Close();
                    }

                    if (IsEnglish)
                    {
                        countryName = "England";
                    }
                    else
                    {
                        countryName = "Germany";
                    }

                    foreach (var item in teamIds)
                    {
                        var query = $"select Count(*) From Countries where name = '{countryName}' and teamId = '{item}'";
                        var insert = $"insert into Countries(name,teamId) values('{countryName}','{item}')";
                        using (var selectCommand = new SqlCommand(query, con))
                        {
                            var rowExist = (int)selectCommand.ExecuteScalar();
                            if (rowExist > 0)
                            {

                                Console.WriteLine("Record already exists!");
                                continue;
                            }
                            else if (rowExist == 0)
                            {
                                using (var insertCommand = new SqlCommand(insert, con))
                                {
                                    insertCommand.ExecuteNonQuery();
                                    Console.WriteLine("Record Pushed to database!");
                                }
                            }
                        }
                    }
                }
                con.Close();
            }
        }
    }
}

