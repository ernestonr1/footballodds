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

        public void PushDataToSeasons()
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

                    if (counter == 1)
                    {
                        div = values[0].ToString();
                        startDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                    }

                    if (reader.EndOfStream)
                    {
                        endDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
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
                using (var command = new SqlCommand(checkForDouble, con))
                {
                    var doubleExist = (int)command.ExecuteScalar();
                    if (doubleExist > 0)
                    {
                        Console.WriteLine("Duplicates found!");
                    }
                    else if (doubleExist == 0)
                    {
                        var insertQuery = $"insert into seasons(startdate,enddate,seasonname,leagueid) values('{startDate}','{endDate}','{fileName}','{leagueId}')";
                        using (var insert = new SqlCommand(insertQuery, con))
                        {
                            insert.ExecuteNonQuery();
                            Console.WriteLine("Record Pushed to database!");
                        }
                    }
                }
                con.Close();
            }

        }

        private static int IdForLeague(string div, int countryId, int leagueId, SqlConnection con)
        {
            var checkForLeagueId = $"select count(*) from leagues where div = ('{div}') and countryId = ('{countryId}')";
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

        private static int IdForCountry(string country, int countryId, SqlConnection con)
        {
            var checkWhichCountry = $"select count(*) from countries where name = ('{country}')";
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

        public void PushDataToMatches()
        {
            var country = "";
            List<DateOnly> MatchDate = new List<DateOnly>();
            List<string> HomeTeam = new List<string>();
            List<string> AwayTeam = new List<string>();
            List<int> FTHG = new List<int>();
            List<int> FTAG = new List<int>();
            List<string> FTR = new List<string>();
            List<int> HTHG = new List<int>();
            List<int> HTAG = new List<int>();
            List<string> HTR = new List<string>();
            List<string> Referee = new List<string>();
            List<int> HS = new List<int>();
            List<int> AS = new List<int>();
            List<int> HST = new List<int>();
            List<int> AST = new List<int>();
            List<int> HF = new List<int>();
            List<int> AF = new List<int>();
            List<int> HC = new List<int>();
            List<int> AC = new List<int>();
            List<int> HY = new List<int>();
            List<int> AY = new List<int>();
            List<int> HR = new List<int>();
            List<int> AR = new List<int>();
            
            using (StreamReader reader = new StreamReader($"{FilePath}"))
            {
                int counter = 0;
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(';', ',');
                    if(counter == 0)
                    {
                        if (values.Contains("Referee"))
                            country = "England";
                        else
                            country = "Germany";
                    }
                    else if(counter > 0)
                    {
                        MatchDate.Add(DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture));
                        HomeTeam.Add(values[2]);
                        AwayTeam.Add(values[3]);
                        
                        
                        //TODO:
                        //kolla upp i teams tabellen id för hometeam & awayteam => spara dessa värden
                        //hämta in leagueId (funktion)
                        //hämta in seasonsId => behöver leagueId, starDatum + slutDatum för säsong => skriv funktion för detta
                        //om land == "Germany" => Referee till null
                    }
                    counter++;
                }
            }
        }
    }
}

