using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlOperations
{
    public class SqlInserts
    {
        public string FilePath { get; set; }
        public string DatabaseConnectionString { get; set; }
        public bool IsEnglish { get; set; }

        public SqlInserts(string filePath, string databaseConnectionString, bool isEnglish)
        {
            FilePath = filePath;
            DatabaseConnectionString = databaseConnectionString;
            IsEnglish = isEnglish;
        }
        public void PushToDatabase()
        {
            PushToCountries();
            PushTeamsToDatabase();
            PushToLeagues();
            PushDataToSeasons();
        }



       private void PushToCountries()
        {
            string countryName = "";
            using (SqlConnection con = new SqlConnection($"{DatabaseConnectionString}"))
            {
                con.Open();
                using (StreamReader reader = new StreamReader($"{FilePath}"))
                {
                    bool keepRunning = true;
                    int counter = 0;
                    while (!reader.EndOfStream && keepRunning)
                    {
                        var line = reader.ReadLine();
                        var values = line.Split(';', ',');
                        if (counter == 0)
                        {
                            if (values.Contains("Referee"))
                                IsEnglish = true;
                            else
                                IsEnglish = false;
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
                            LookUpAndInsertData(con, query, insert);
                        }
                        keepRunning = false;
                    }
                    con.Close();
                }
            }
        }
       private void PushTeamsToDatabase()
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
                        if (counter == 0)
                        {
                            if (values.Contains("Referee"))
                                IsEnglish = true;
                            else
                                IsEnglish = false;
                        }
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


                int idForCountry = 0;
                string id = "";
                if (IsEnglish)
                    id = "England";
                else
                    id = "Germany";
                var idForTeam = $"select id from Countries where name = ('{id}')";
                using (var correctId = new SqlCommand(idForTeam, con))
                {
                    int rowExist = (int)correctId.ExecuteScalar();
                    if (rowExist > 0)
                    {
                        idForCountry = rowExist;
                        Console.WriteLine("Record exists!");
                    }
                }

                //not much to say, first we check if the record is present in the database, if it's we continue the loop and print a message,
                //otherwise we push it to the database with a message.
                foreach (var item in result)
                {

                    var query = $"Select count(*) from Teams where TeamName = @item and CountryId = @idForCountry";
                    var insert = $"insert into Teams(TeamName,CountryId) values(@item,@idForCountry)";
                    using (var command = new SqlCommand(query, con))
                    {
                        command.Parameters.AddWithValue("@item", item);
                        command.Parameters.AddWithValue("@idForCountry", idForCountry);

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
                                myInsert.Parameters.AddWithValue("@item", item);
                                myInsert.Parameters.AddWithValue("@idForCountry", idForCountry);
                                myInsert.ExecuteNonQuery();
                                Console.WriteLine("Record Pushed to database!");
                            }
                        }
                    }
                }
                con.Close();
            }
        }
       private void PushToLeagues()
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
                    CheckCountry(counter, values);
                    if (IsEnglish)
                        country = "England";
                    else
                        country = "Germany";
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

                var teamId = $"select id from Countries where name = ('{country}')";
                using (var command = new SqlCommand(teamId, con))
                {
                    int rowExist = (int)command.ExecuteScalar();
                    if (rowExist > 0)
                    {
                        Console.WriteLine("Found correct Id!");
                        var insert = $"insert into Leagues(div,countryId) values('{leagues[0]}','{rowExist}')";
                        var checkForDoubles = $"select count(*) from Leagues where div = ('{leagues[0]}') and countryId = ('{rowExist}')";
                        LookUpAndInsertData(con, checkForDoubles, insert);
                    }
                }
            }
        }
       private void PushDataToSeasons()
        {
            string div = "";
            string start = "";
            string end = "";
            string country = "";
            int countryId = 0;
            int leagueId = 0;
            var fileName = Path.GetFileNameWithoutExtension($"{FilePath}");
            DateOnly startDate;
            DateOnly endDate;

            //parsing the end and startingdate to dateonly and country with correct value
            using (StreamReader reader = new StreamReader($"{FilePath}"))
            {
                int counter = 0;
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(';', ',');
                    CheckCountry(counter, values);

                    if (counter == 1)
                    {
                        div = values[0].ToString();
                        if (values[1].Length <= 8)
                        {
                            startDate = ConvertDate(values);
                        }
                        else
                        {
                            startDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                        }
                    }




                    if (reader.EndOfStream)
                    {
                        if (values[1].Length <= 8)
                            endDate = ConvertDate(values);
                        else
                        {
                            endDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                        }
                    }
                    counter++;
                }
            }

            if (IsEnglish)
                country = "England";
            else
                country = "Germany";

            using (var con = new SqlConnection($"{DatabaseConnectionString}"))
            {
                con.Open();
                //retrieving correct id for the country
                countryId = IdForCountry(country, countryId, con);
                //retrieving the leagueId
                leagueId = IdForLeague(div, countryId, leagueId, con);
                //checking for duplicates and pushing to database if no duplicates was found
                var checkForDouble = $"select Count(*) from Seasons where startDate = ('{startDate}') and endDate = ('{endDate}') and leagueId = ('{leagueId}')";
                var insertQuery = $"insert into seasons(startdate,enddate,seasonname,leagueid) values('{startDate}','{endDate}','{fileName}','{leagueId}')";
                LookUpAndInsertData(con, checkForDouble, insertQuery);
                con.Close();
            }

        }



        private bool CheckCountry(int counter, string[] values)
        {
            if (counter == 0)
            {
                if (values.Contains("Referee"))
                {
                    IsEnglish = true;
                }
                else
                {
                    IsEnglish = false;
                }
            }

            return IsEnglish;
        }
        protected  void LookUpAndInsertData(SqlConnection con, string query, string insert)
        {
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
        }
        protected  int IdForLeague(string div, int countryId, int leagueId, SqlConnection con)
        {
            var checkForLeagueId = $"select id from leagues where div = ('{div}') and countryId = ('{countryId}')";
            using (var command = new SqlCommand(checkForLeagueId, con))
            {
                int doubleExist = (int)command.ExecuteScalar();
                if (doubleExist > 0)
                {
                    leagueId = doubleExist;
                }
            }
            return leagueId;
        }
        protected  DateOnly ConvertDate(string[] values)
        {
            DateOnly matchDate;
            string newDate = values[1];
            var newNewDate = newDate.Split('/');
            var year = "20";
            year += newNewDate[2];
            var newNewNewDate = $"{newNewDate[0]}/{newNewDate[1]}/{year}";
            matchDate = DateOnly.ParseExact(newNewNewDate, "dd/MM/yyyy", CultureInfo.InvariantCulture);
            return matchDate;
        }
        private static int IdForCountry(string country, int countryId, SqlConnection con)
        {
            var checkWhichCountry = $"select id from countries where name = ('{country}')";
            using (var checkCountry = new SqlCommand(checkWhichCountry, con))
            {
                int doubleExists = (int)checkCountry.ExecuteScalar();
                if (doubleExists > 0)
                {
                    countryId = doubleExists;
                }
            }
            return countryId;
        }
    }
}

