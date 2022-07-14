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

        public SqlInserts(string filePath, string databaseConnectionString, bool isEnglish)
        {
            FilePath = filePath;
            DatabaseConnectionString = databaseConnectionString;
            IsEnglish = isEnglish;
        }

        public void PushTeamsToDatabase()
        {
            List<string> firstRow = new List<string>();
            List<string> result = new List<string>();

            using (SqlConnection con = new SqlConnection($"{DatabaseConnectionString}"))
            {
                con.Open();

                int idForCountry = 0;
                string id = "";
                if (IsEnglish)
                    id = "England";
                else
                    id = "Germany";
                var idForTeam = $"select Count(*) from Countries where name = ('{id}')";
                using (var correctId = new SqlCommand(idForTeam, con))
                {
                    int rowExist = (int)correctId.ExecuteScalar();
                    if (rowExist > 0)
                    {
                        idForCountry = rowExist;
                        Console.WriteLine("Record exists!");
                    }
                }
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
                    var query = $"Select Count(*) from Teams where TeamName = ('{item}') and CountryId = ('{idForCountry}')";

                    var insert = $"insert into Teams(TeamName,CountryId) values('{item}','{idForCountry}')";
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
            string countryName = "";
            using (SqlConnection con = new SqlConnection($"{DatabaseConnectionString}"))
            {
                con.Open();
                if (IsEnglish)
                {
                    countryName = "England";
                }
                else
                {
                    countryName = "Germany";
                }
                var query = $"select Count(*) From Countries where name = '{countryName}'";
                var insert = $"insert into Countries(name) values('{countryName}')";
                using (var selectCommand = new SqlCommand(query, con))
                {
                    var rowExist = (int)selectCommand.ExecuteScalar();
                    if (rowExist > 0)
                    {
                        Console.WriteLine("Record already exists!");
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
                con.Close();
            }
        }

        public void PushToLeagues()
        {
            List<string> leagues = new List<string>();
            string country = "";
            using (StreamReader reader = new StreamReader($"{FilePath}"))
            {
                int counter = 0;
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(';', ',');
                    if (values.Contains("Referee"))
                    {
                        country = "England";
                    }
                    if (counter > 0)
                    {
                        leagues.Add(values[0].ToString());
                        break;
                    }
                    counter++;
                }

            }

            using (var con = new SqlConnection($"{DatabaseConnectionString}"))
            {
                con.Open();


                var teamId = $"select Count(*) from Countries where name = ('{country}')";
                using (var command = new SqlCommand(teamId, con))
                {
                    int rowExist = (int)command.ExecuteScalar();
                    if (rowExist > 0)
                    {
                        Console.WriteLine("Found correct Id!");
                        var insert = $"insert into Leagues(div,countryId) values('{leagues[0]}','{rowExist}')";
                        var checkForDoubles = $"select count(*) from Leagues where div = ('{leagues[0]}') and countryId = ('{rowExist}')";
                        using (var checkForDoublesCommand = new SqlCommand(checkForDoubles, con))
                        {
                            int doubleExists = (int)checkForDoublesCommand.ExecuteScalar();
                            if (doubleExists > 0)
                            {
                                Console.WriteLine("Duplicates found!");
                            }
                            else if (doubleExists == 0)
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
            }
        }
    }
}

