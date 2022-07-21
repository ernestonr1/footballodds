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

        public void PushToCountries()
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
                    CheckCountry(counter, values);
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


        public void PushDataToMatches()
        {
            var country = "";
            int countryId = 0;
            int leagueId = 0;
            int seasonId = 0;
            DateOnly MatchDate;
            DateOnly StartDate;
            DateOnly EndDate;
            string HomeTeam = "";
            string AwayTeam = "";
            string div = "";
            int homeTeamId = 0;
            int awayTeamId = 0;
            int FTHGIndex = 0;
            int HTRIndex = 0;
            int HSIndex = 0;
            int ARIndex = 0;


            int FTHG = 0;
            int FTAG = 0;
            string FTR = "";
            int HTHG = 0;
            int HTAG = 0;
            string HTR = "";
            string Referee = "";
            int HS = 0;
            int AS = 0;
            int HST = 0;
            int AST = 0;
            int HF = 0;
            int AF = 0;
            int HC = 0;
            int AC = 0;
            int HY = 0;
            int AY = 0;
            int HR = 0;
            int AR = 0;

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
                        {
                            country = "England";
                            IsEnglish = true;
                        }
                        else
                        {
                            country = "Germany";
                            IsEnglish = false;
                        }


                        int firstIndex = Array.IndexOf(values, "FTHG");
                        FTHGIndex = firstIndex;
                        Console.WriteLine("FTHG Index found");

                        int secondIndex = Array.IndexOf(values, "HTR");
                        HTRIndex = secondIndex;
                        Console.WriteLine("HTR Index found");

                        int thirdIndex = Array.IndexOf(values, "HS");
                        HSIndex = thirdIndex;
                        Console.WriteLine("Bet365 Index found");

                        int fourthIndex = Array.IndexOf(values, "AR");
                        ARIndex = firstIndex;
                        Console.WriteLine("AR Index found");
                    }


                    if (counter == 1)
                    {
                        if (values[1].Length <= 8)
                        {
                            StartDate = ConvertDate(values);
                        }
                        else
                        {
                            StartDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                        }
                        div = values[0].ToString();


                    }

                    if (counter > 0)
                    {
                        if (values[1].Length <= 8)
                        {
                            MatchDate = ConvertDate(values);
                        }
                        else
                        {
                            MatchDate = (DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture));
                        }
                        HomeTeam = (values[2]);
                        AwayTeam = (values[3]);
                        using (var con = new SqlConnection($"{DatabaseConnectionString}"))
                        {
                            con.Open();


                            //check home team
                            var checkOnHomeTeam = $"select id from teams where teamname = @HomeTeam";
                            using (var checkOnTeam = new SqlCommand(checkOnHomeTeam, con))
                            {
                                checkOnTeam.Parameters.AddWithValue("@HomeTeam", HomeTeam);
                                int doubleExists = (int)checkOnTeam.ExecuteScalar();
                                if (doubleExists > 0)
                                    homeTeamId = doubleExists;
                            }
                            //check away team
                            var checkOnAwayTeam = $"select id from teams where teamname = @AwayTeam";
                            using (var checkOnAwayTeamId = new SqlCommand(checkOnAwayTeam, con))
                            {
                                checkOnAwayTeamId.Parameters.AddWithValue("@AwayTeam", AwayTeam);

                                int doubleExists = (int)checkOnAwayTeamId.ExecuteScalar();
                                if (doubleExists > 0)
                                    awayTeamId = doubleExists;
                            }
                            var checkCountry = $"select id from countries where name = ('{country}')";
                            using (var checkOnCountry = new SqlCommand(checkCountry, con))
                            {
                                int doubleExist = (int)checkOnCountry.ExecuteScalar();
                                if (doubleExist > 0)
                                    countryId = doubleExist;
                            }

                            leagueId = IdForLeague(div, countryId, leagueId, con);
                            var checkSeason = $"select id from seasons where startdate = ('{StartDate}') and leagueId = ('{leagueId}')";
                            using (var checkOnSeason = new SqlCommand(checkSeason, con))
                            {
                                int doubleExist = (int)checkOnSeason.ExecuteScalar();
                                if (doubleExist > 0)
                                    seasonId = doubleExist;
                            }
                            if (country == "England")
                            {
                                FTHG = Convert.ToInt32(values[FTHGIndex]);
                                FTAG = Convert.ToInt32(values[FTHGIndex + 1]);
                                FTR = values[FTHGIndex + 2];
                                HTHG = Convert.ToInt32(values[FTHGIndex + 3]);
                                HTAG = Convert.ToInt32(values[FTHGIndex + 4]);
                                HTR = values[HTRIndex];
                                Referee = values[HTRIndex + 1];
                                HS = Convert.ToInt32(values[HSIndex]);
                                AS = Convert.ToInt32(values[HSIndex + 1]);
                                HST = Convert.ToInt32(values[HSIndex + 2]);
                                AST = Convert.ToInt32(values[HSIndex + 3]);
                                HF = Convert.ToInt32(values[HSIndex + 4]);
                                AF = Convert.ToInt32(values[HSIndex + 5]);
                                HC = Convert.ToInt32(values[HSIndex + 6]);
                                AC = Convert.ToInt32(values[HSIndex + 7]);
                                HY = Convert.ToInt32(values[HSIndex + 8]);
                                AY = Convert.ToInt32(values[HSIndex + 9]);
                                HR = Convert.ToInt32(values[HSIndex + 10]);
                                AR = Convert.ToInt32(values[ARIndex]);
                            }
                            if (country == "Germany")
                            {
                                FTHG = Convert.ToInt32(values[FTHGIndex]);
                                FTAG = Convert.ToInt32(values[FTHGIndex + 1]);
                                FTR = values[FTHGIndex + 2];
                                HTHG = Convert.ToInt32(values[FTHGIndex + 3]);
                                HTAG = Convert.ToInt32(values[FTHGIndex + 4]);
                                HTR = values[HTRIndex];
                                HS = Convert.ToInt32(values[HSIndex]);
                                AS = Convert.ToInt32(values[HSIndex + 1]);
                                HST = Convert.ToInt32(values[HSIndex + 2]);
                                AST = Convert.ToInt32(values[HSIndex + 3]);
                                HF = Convert.ToInt32(values[HSIndex + 4]);
                                AF = Convert.ToInt32(values[HSIndex + 5]);
                                HC = Convert.ToInt32(values[HSIndex + 6]);
                                AC = Convert.ToInt32(values[HSIndex + 7]);
                                HY = Convert.ToInt32(values[HSIndex + 8]);
                                AY = Convert.ToInt32(values[HSIndex + 9]);
                                HR = Convert.ToInt32(values[HSIndex + 10]);
                                AR = Convert.ToInt32(values[ARIndex]);
                            }
                            //var insertOperationOne = $"insert into Matches(homeTeamId,awayTeamId,seasonId,matchDate,FTHG,FTAG,FTR,HTHG,HTAG,HTR,REFEREE) values ('{homeTeamId}','{awayTeamId}','{seasonId}','{MatchDate}','{FTHG}','{FTAG}','{FTR}','{HTHG}','{HTAG}','{HTR}','{Referee}'}')";

                            var checkInsertOperation = $"select count(*) from Matches where homeTeamId = ('{homeTeamId}') and awayTeamId = ('{awayTeamId}') and seasonId = ('{seasonId}')" +
                                $" and  matchdate = ('{MatchDate}') and fthg = ('{FTHG}') and ftag = ('{FTAG}') and ftr = ('{FTR}') and hthg = ('{HTHG}')" +
                                $" and htag = ('{HTAG}') and htr = ('{HTR}') and referee = ('{Referee}') and hs = ('{HS}') and [as] = ('{AS}') and hst = ('{HST}') and ast = ('{AST}')" +
                                $"and hf = ('{HF}') and af = ('{AF}') and hc = ('{HC}') and ac = ('{AC}') and hy = ('{HY}') and ay = ('{AY}') and hr = ('{HR}') and ar = ('{AR}')";
                            var insertOperation = $"insert into Matches(homeTeamId,awayTeamId,seasonId,matchDate,FTHG,FTAG,FTR,HTHG,HTAG,HTR,REFEREE,HS,[AS],HST,AST,HF,AF,HC,AC,HY,AY,HR,AR) values ('{homeTeamId}','{awayTeamId}','{seasonId}','{MatchDate}','{FTHG}','{FTAG}','{FTR}','{HTHG}','{HTAG}','{HTR}','{Referee}','{HS}','{AS}','{HST}','{AST}','{HF}','{AF}','{HC}','{AC}','{HY}','{AY}','{HR}','{AR}')";

                            var insertOperationGermany = $"insert into Matches(homeTeamId,awayTeamId,seasonId,matchDate,FTHG,FTAG,FTR,HTHG,HTAG,HTR,REFEREE,HS,[AS],HST,AST,HF,AF,HC,AC,HY,AY,HR,AR) " +
                                $"values (@homeTeamId,@awayTeamId,@seasonId,'{MatchDate}',@FTHG,@FTAG,@FTR,@HTHG,@HTAG,@HTR,@Referee,@HS,@AS,@HST,@AST,@HF,@AF,@HC,@AC,@HY,@AY,@HR,@AR)";

                            if (country == "England")
                            {
                                LookUpAndInsertData(con, checkInsertOperation, insertOperation);
                            }
                            else
                            {
                                using (var inputResult = new SqlCommand(checkInsertOperation, con))
                                {
                                    var rowExist = (int)inputResult.ExecuteScalar();
                                    if (rowExist > 0)
                                    {
                                        Console.WriteLine("Record already exists!");
                                    }
                                    else if (rowExist == 0)
                                    {
                                        using (var insertCommand = new SqlCommand(insertOperationGermany, con))
                                        {

                                            insertCommand.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                            insertCommand.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                            insertCommand.Parameters.AddWithValue("@seasonId", seasonId);
                                            insertCommand.Parameters.AddWithValue("@FTHG", FTHG);
                                            insertCommand.Parameters.AddWithValue("@FTAG", FTAG);
                                            insertCommand.Parameters.AddWithValue("@FTR", FTR);
                                            insertCommand.Parameters.AddWithValue("@HTHG", HTHG);
                                            insertCommand.Parameters.AddWithValue("@HTAG", HTAG);
                                            insertCommand.Parameters.AddWithValue("@HTR", HTR);
                                            insertCommand.Parameters.AddWithValue("@Referee", DBNull.Value);
                                            insertCommand.Parameters.AddWithValue("@HS", HS);
                                            insertCommand.Parameters.AddWithValue("@AS", AS);
                                            insertCommand.Parameters.AddWithValue("@HST", HST);
                                            insertCommand.Parameters.AddWithValue("@AST", AST);
                                            insertCommand.Parameters.AddWithValue("@HF", HF);
                                            insertCommand.Parameters.AddWithValue("@AF", AF);
                                            insertCommand.Parameters.AddWithValue("@HC", HC);
                                            insertCommand.Parameters.AddWithValue("@AC", AC);
                                            insertCommand.Parameters.AddWithValue("@HY", HY);
                                            insertCommand.Parameters.AddWithValue("@AY", AY);
                                            insertCommand.Parameters.AddWithValue("@HR", HR);
                                            insertCommand.Parameters.AddWithValue("@AR", AR);
                                            insertCommand.ExecuteNonQuery();
                                            Console.WriteLine("Record Pushed to database!");
                                        }
                                    }
                                }
                            }
                            //if not england => insert referee null
                        }
                    }
                    counter++;
                }
            }
        }

        public void PushDataToBettingCompanies()
        {
            List<string> bettingList = new List<string>
            {
                    "Bet365",
                    "Blue Square",
                    "Bet&Win",
                    "Gamebookers",
                    "Interwetten",
                    "Ladbrokes",
                    "Pinnacle",
                    "Sporting Odds",
                    "Sportingbet",
                    "Stan James",
                    "Stanleybet",
                    "VC Bet",
                    "William Hill"
            };

            using (var con = new SqlConnection($"{DatabaseConnectionString}"))
            {
                con.Open();

                foreach (var item in bettingList)
                {

                    var bettingCompanyName = $"select count(*) from BettingCompanies where BettingCompanyName = ('{item}')";
                    using (var selectCommand = new SqlCommand(bettingCompanyName, con))
                    {
                        int doubleItems = (int)selectCommand.ExecuteScalar();
                        if (doubleItems > 0)
                        {
                            Console.WriteLine("Duplicates found!");
                            continue;
                        }
                        else if (doubleItems == 0)
                        {
                            var bettingCompanyInsert = $"insert into bettingcompanies(BettingCompanyName) values('{item}')";
                            using (var insertCommand = new SqlCommand(bettingCompanyInsert, con))
                            {
                                insertCommand.ExecuteNonQuery();
                                Console.WriteLine("Record Pushed to database!");
                            }
                        }
                    }
                }
                con.Close();
            }
        }
        public void PushMatchOdds()
        {
            CultureInfo culture = CultureInfo.InvariantCulture;
            string homeTeam = "";
            string awayTeam = "";
            int homeTeamId = 0;
            int awayTeamId = 0;
            int matchId = 0;
            DateOnly matchDate;
            string? nullValue = "";

            int bet365Id = 0;
            int betAndWinId = 0;
            int interwettenId = 0;
            int ladbrokersId = 0;
            int pinnacleId = 0;
            int williamHillId = 0;
            int VCBetId = 0;

            int bet365Index = 0;
            int betAndWinIndex = 0;
            int interwettenIndex = 0;
            int ladbrokersIndex = 0;
            int pinnacleIndex = 0;
            int williamHillIndex = 0;
            int VCIndex = 0;

            decimal homeWin = 0M;
            decimal draw = 0M;
            decimal awayWin = 0M;

            List<string> matchOdds = new List<string>();
            using (var con = new SqlConnection($"{DatabaseConnectionString}"))
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
                            {
                                IsEnglish = true;
                            }
                            else
                            {
                                IsEnglish = false;
                            }
                            StartIndexesForBettingCompanies(out bet365Index, out betAndWinIndex, out interwettenIndex, out ladbrokersIndex, out pinnacleIndex, out williamHillIndex, out VCIndex, values);
                        }

                        if (counter > 0)
                        {
                            if (values[1].Length <= 8)
                            {
                                matchDate = ConvertDate(values);
                            }
                            else
                            {
                                matchDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                            }
                            homeTeam = values[2];
                            awayTeam = values[3];

                            var homeTeamIdCheck = $"select id from Teams where teamname = @homeTeam";
                            using (var home = new SqlCommand(homeTeamIdCheck, con))
                            {
                                home.Parameters.AddWithValue("@homeTeam", homeTeam);
                                int doubleExists = (int)home.ExecuteScalar();
                                if (doubleExists > 0)
                                {
                                    homeTeamId = doubleExists;
                                }
                            }

                            var awayTeamIdCheck = $"select id from Teams where teamname = @awayTeam";
                            using (var away = new SqlCommand(awayTeamIdCheck, con))
                            {
                                away.Parameters.AddWithValue("@awayTeam", awayTeam);
                                int doubleExist = (int)away.ExecuteScalar();
                                if (doubleExist > 0)
                                {
                                    awayTeamId = doubleExist;
                                }
                            }

                            var matchIdCheck = $"select id from Matches where awayTeamId = ('{awayTeamId}') and homeTeamId = ('{homeTeamId}') and matchDate = ('{matchDate}')";
                            using (var match = new SqlCommand(matchIdCheck, con))
                            {
                                int doubleExist = (int)match.ExecuteScalar();
                                if (doubleExist > 0)
                                {
                                    matchId = doubleExist;
                                }
                            }


                            var bettingIdCheck = $"select count(*) from MatchOdds where matchId = ('{matchId}') and awayTeamId = ('{awayTeamId}') and homeTeamId = ('{homeTeamId}') and matchDate = ('{matchDate}')";
                            using (var betting = new SqlCommand(bettingIdCheck, con))
                            {
                                var doubleExist = (int)betting.ExecuteScalar();
                                if (doubleExist > 0)
                                {
                                    Console.WriteLine("Duplicates found!");
                                }
                                if (doubleExist == 0)
                                {
                                    var bet365Select = $"select id from bettingcompanies where BettingCompanyName = ('Bet365')";
                                    using (var bet365 = new SqlCommand(bet365Select, con))
                                    {

                                        //CheckForNullAndEmptyValue(culture, ref nullValue, bet365Index, ref homeWin, ref draw, ref awayWin, values);


                                        if (bet365Index == -1 || values[bet365Index] == "")
                                        {
                                            nullValue = null;
                                        }
                                        else
                                        {
                                            homeWin = decimal.Parse(values[bet365Index], culture);
                                            draw = decimal.Parse(values[bet365Index + 1], culture);
                                            awayWin = decimal.Parse(values[bet365Index + 2], culture);
                                        }

                                        int doubleExist365 = (int)bet365.ExecuteScalar();
                                        if (doubleExist365 > 0)
                                        {
                                            bet365Id = doubleExist365;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) values (@matchId,@bet365Id,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {

                                                if (bet365Index == -1 || values[bet365Index] == "")
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@bet365Id", bet365Id);
                                                    inputResult.Parameters.AddWithValue("@home", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@draw", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();

                                                }
                                                else if (bet365Index != -1)
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@bet365Id", bet365Id);
                                                    inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                    inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                    inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();

                                                }

                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }

                                    var betandwinSelect = $"select id from bettingcompanies where BettingCompanyName = ('Bet&Win')";
                                    using (var betandwin = new SqlCommand(betandwinSelect, con))
                                    {

                                        if (betAndWinIndex == -1 || values[betAndWinIndex] == "")
                                        {
                                            nullValue = null;
                                        }
                                        else
                                        {
                                            homeWin = decimal.Parse(values[betAndWinIndex], culture);
                                            draw = decimal.Parse(values[betAndWinIndex + 1], culture);
                                            awayWin = decimal.Parse(values[betAndWinIndex + 2], culture);
                                        }

                                        //CheckForNullAndEmptyValue(culture, ref nullValue, betAndWinIndex, ref homeWin, ref draw, ref awayWin, values);
                                        int doubleExistBetAndWin = (int)betandwin.ExecuteScalar();

                                        if (doubleExistBetAndWin > 0)
                                        {
                                            betAndWinId = doubleExistBetAndWin;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@betAndWinId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {
                                                if (betAndWinIndex == -1 || values[betAndWinIndex] == "")
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@betAndWinId", betAndWinId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@draw", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();
                                                }
                                                else if (betAndWinIndex != -1)
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@betAndWinId", betAndWinId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                    inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                    inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();
                                                }
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }

                                    var interwettenSelect = $"select id from bettingcompanies where BettingCompanyName = ('Interwetten')";
                                    using (var interwetten = new SqlCommand(interwettenSelect, con))
                                    {

                                        if (interwettenIndex == -1 || values[interwettenIndex] == "")
                                        {
                                            nullValue = null;
                                        }
                                        else
                                        {
                                            homeWin = decimal.Parse(values[interwettenIndex], culture);
                                            draw = decimal.Parse(values[interwettenIndex + 1], culture);
                                            awayWin = decimal.Parse(values[interwettenIndex + 2], culture);
                                        }

                                        //CheckForNullAndEmptyValue(culture, ref nullValue, interwettenIndex, ref homeWin, ref draw, ref awayWin, values);
                                        int doubleExistInterwetten = (int)interwetten.ExecuteScalar();

                                        if (doubleExistInterwetten > 0)
                                        {
                                            interwettenId = doubleExistInterwetten;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@interwettenId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {
                                                // AddParameterizedValues(homeTeamId, awayTeamId, matchId, interwettenId, interwettenIndex, homeWin, draw, awayWin, values, inputResult);


                                                if (interwettenIndex == -1 || values[interwettenIndex] == "")
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@interwettenId", interwettenId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@draw", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();

                                                }
                                                else if (interwettenIndex != -1)
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@interwettenId", interwettenId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                    inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                    inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();
                                                }


                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }

                                    var ladbrokersSelect = $"select id from bettingcompanies where BettingCompanyName = ('Ladbrokes')";
                                    using (var ladbrokers = new SqlCommand(ladbrokersSelect, con))
                                    {

                                        if (ladbrokersIndex == -1 || values[ladbrokersIndex] == "")
                                        {
                                            nullValue = null;
                                        }
                                        else
                                        {
                                            homeWin = decimal.Parse(values[ladbrokersIndex], culture);
                                            draw = decimal.Parse(values[ladbrokersIndex + 1], culture);
                                            awayWin = decimal.Parse(values[ladbrokersIndex + 2], culture);
                                        }

                                        //CheckForNullAndEmptyValue(culture, ref nullValue, ladbrokersIndex, ref homeWin, ref draw, ref awayWin, values);

                                        int doubleExistLadbrokers = (int)ladbrokers.ExecuteScalar();
                                        if (doubleExistLadbrokers > 0)
                                        {
                                            ladbrokersId = doubleExistLadbrokers;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@ladbrokersId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";

                                            using (var inputResult = new SqlCommand(input, con))
                                            {
                                                //AddParameterizedValues(homeTeamId, awayTeamId, matchId, ladbrokersId, ladbrokersIndex, homeWin, draw, awayWin, values, inputResult);

                                                if (ladbrokersIndex == -1 || values[ladbrokersIndex] == "")
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@ladbrokersId", ladbrokersId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@draw", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();
                                                }
                                                else if (ladbrokersIndex != -1)
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@ladbrokersId", ladbrokersId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                    inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                    inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();
                                                }
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }

                                    var pinnacleSelect = $"select id from bettingcompanies where BettingCompanyName = ('Pinnacle')";
                                    using (var pinnacle = new SqlCommand(pinnacleSelect, con))
                                    {

                                        if (pinnacleIndex == -1 || values[pinnacleIndex] == "")
                                        {
                                            nullValue = null;
                                        }
                                        else
                                        {
                                            homeWin = decimal.Parse(values[pinnacleIndex], culture);
                                            draw = decimal.Parse(values[pinnacleIndex + 1], culture);
                                            awayWin = decimal.Parse(values[pinnacleIndex + 2], culture);
                                        }

                                        //CheckForNullAndEmptyValue(culture, ref nullValue, pinnacleIndex, ref homeWin, ref draw, ref awayWin, values);
                                        int doubleExistPinnacle = (int)pinnacle.ExecuteScalar();

                                        if (doubleExistPinnacle > 0)
                                        {
                                            pinnacleId = doubleExistPinnacle;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@pinnacleId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {


                                                if (pinnacleIndex == -1 || values[pinnacleIndex] == "")
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@pinnacleId", pinnacleId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@draw", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();
                                                }
                                                else if (pinnacleIndex != -1)
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@pinnacleId", pinnacleId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                    inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                    inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();
                                                }

                                                //AddParameterizedValues(homeTeamId, awayTeamId, matchId, pinnacleId, pinnacleIndex, homeWin, draw, awayWin, values, inputResult);
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }
                                    var williamHillSelect = $"select id from bettingcompanies where BettingCompanyName = ('William Hill')";
                                    using (var williamHill = new SqlCommand(williamHillSelect, con))
                                    {

                                        if (williamHillIndex == -1 || values[williamHillIndex] == "")
                                        {
                                            nullValue = null;
                                        }
                                        else
                                        {
                                            homeWin = decimal.Parse(values[williamHillIndex], culture);
                                            draw = decimal.Parse(values[williamHillIndex + 1], culture);
                                            awayWin = decimal.Parse(values[williamHillIndex + 2], culture);
                                        }

                                        //CheckForNullAndEmptyValue(culture, ref nullValue, williamHillIndex, ref homeWin, ref draw, ref awayWin, values);
                                        int doubleExistWilliamHill = (int)williamHill.ExecuteScalar();

                                        if (doubleExistWilliamHill > 0)
                                        {
                                            williamHillId = doubleExistWilliamHill;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@williamHillId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {


                                                if (williamHillIndex == -1 || values[williamHillIndex] == "")
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@williamHillId", williamHillId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@draw", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();

                                                }
                                                else if (williamHillIndex != -1)
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@williamHillId", williamHillId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                    inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                    inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();

                                                }

                                                // AddParameterizedValues(homeTeamId, awayTeamId, matchId, williamHillId, williamHillIndex, homeWin, draw, awayWin, values, inputResult);
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }


                                    var VCBetSelect = $"select id from bettingcompanies where BettingCompanyName = ('VC Bet')";
                                    using (var VCBet = new SqlCommand(VCBetSelect, con))
                                    {


                                        if (VCIndex == -1 || values[VCIndex] == "")
                                        {
                                            nullValue = null;
                                        }
                                        else
                                        {
                                            homeWin = decimal.Parse(values[VCIndex], culture);
                                            draw = decimal.Parse(values[VCIndex + 1], culture);
                                            awayWin = decimal.Parse(values[VCIndex + 2], culture);
                                        }

                                        //CheckForNullAndEmptyValue(culture, ref nullValue, VCIndex, ref homeWin, ref draw, ref awayWin, values);
                                        int doubleExistVCBet = (int)VCBet.ExecuteScalar();

                                        if (doubleExistVCBet > 0)
                                        {
                                            VCBetId = doubleExistVCBet;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@VCBetId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {


                                                if (VCIndex == -1 || values[VCIndex] == "")
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@VCBetId", VCBetId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@draw", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();

                                                }
                                                else if (VCIndex != -1)
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@VCBetId", VCBetId);
                                                    inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                    inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                    inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                    inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                    inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                    inputResult.ExecuteNonQuery();

                                                }

                                                //  AddParameterizedValues(homeTeamId, awayTeamId, matchId, VCBetId, VCIndex, homeWin, draw, awayWin, values, inputResult);
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }
                                    matchOdds.Clear();
                                }
                            }
                        }
                        counter++;
                    }
                    reader.Close();
                }
            }
        }

        public void PushSpecificMatchDate()
        {
            DateOnly MatchDate;
            string HomeTeam = "";
            string AwayTeam = "";
            int matchId = 0;
            int homeTeamId = 0;
            int awayTeamId = 0;
            int startIndex = 0;
            CultureInfo culture = CultureInfo.InvariantCulture;
            List<string> specificList = new List<string>();

            using (StreamReader reader = new StreamReader($"{FilePath}"))
            {
                int counter = 0;
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(';', ',');

                    if (counter == 0)
                    {
                        int index = Array.IndexOf(values, "Bb1X2");
                        startIndex = index;
                        Console.WriteLine("Bb1X2 found");
                        if (values.Contains("Referee"))
                        {
                            IsEnglish = true;
                        }
                        else
                        {
                            IsEnglish = false;
                        }
                    }

                    else if (counter > 0)
                    {
                        for (int i = startIndex; i < values.Length; i++)
                        {
                            specificList.Add(values[i].ToString());
                        }

                        if (values[1].Length <= 8)
                        {
                            string newDate = values[1];
                            var newNewDate = newDate.Split('/');
                            var year = "20";
                            year += newNewDate[2];
                            var newNewNewDate = $"{newNewDate[0]}/{newNewDate[1]}/{year}";
                            MatchDate = DateOnly.ParseExact(newNewNewDate, "dd/MM/yyyy", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            MatchDate = (DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture));
                        }
                        HomeTeam = (values[2]);
                        AwayTeam = (values[3]);

                        using (var con = new SqlConnection($"{DatabaseConnectionString}"))
                        {
                            con.Open();
                            var homeTeamCheck = $"select id from teams where teamname = @HomeTeam";
                            using (var first = new SqlCommand(homeTeamCheck, con))
                            {
                                first.Parameters.AddWithValue("@HomeTeam", HomeTeam);
                                int doubleValues = (int)first.ExecuteScalar();
                                if (doubleValues > 0)
                                    homeTeamId = doubleValues;

                            }
                            var awayTeamCheck = $"select id from teams where teamname = @AwayTeam";
                            using (var first = new SqlCommand(awayTeamCheck, con))
                            {
                                first.Parameters.AddWithValue("@AwayTeam", AwayTeam);
                                int doubleValues = (int)first.ExecuteScalar();
                                if (doubleValues > 0)
                                    awayTeamId = doubleValues;
                            }

                            var matchDateIdCheck = $"select id from matches where homeTeamId = ('{homeTeamId}') and awayTeamId = ('{awayTeamId}') and matchDate = ('{MatchDate}')";
                            using (var first = new SqlCommand(matchDateIdCheck, con))
                            {
                                int doubleValues = (int)first.ExecuteScalar();
                                if (doubleValues > 0)
                                    matchId = doubleValues;
                            }

                            //kolla upp dubletter
                            var doubleRecord = $"select count(*) from SpecificBettingOdds where homeTeamId = ('{homeTeamId}') and awayTeamId = ('{awayTeamId}') and matchDate = ('{MatchDate}') and matchId = ('{matchId}')";
                            using (var first = new SqlCommand(doubleRecord, con))
                            {
                                int doubleValues = (int)first.ExecuteScalar();
                                if (doubleValues > 0)
                                {
                                    Console.WriteLine("Duplicates found!");
                                }
                                else if (doubleValues == 0)
                                {
                                    double bB1X2 = double.Parse(specificList[0], culture);
                                    double BbMxH = double.Parse(specificList[1], culture);
                                    double BbAvH = double.Parse(specificList[2], culture);
                                    double BbMxD = double.Parse(specificList[3], culture);
                                    double BbAvD = double.Parse(specificList[4], culture);
                                    double BbMxA = double.Parse(specificList[5], culture);
                                    double BbAvA = double.Parse(specificList[6], culture);
                                    double BbOU = double.Parse(specificList[7], culture);
                                    double BbMxGT25 = double.Parse(specificList[8], culture);
                                    double BbAvGT25 = double.Parse(specificList[9], culture);
                                    double BbMxLT25 = double.Parse(specificList[10], culture);
                                    double BbAvLT25 = double.Parse(specificList[11], culture);
                                    double BbAH = double.Parse(specificList[12], culture);
                                    double BbAHH = double.Parse(specificList[13], culture);
                                    double BbMxAHH = double.Parse(specificList[14], culture);
                                    double BbAvAHH = double.Parse(specificList[15], culture);
                                    double BbMxAHA = double.Parse(specificList[16], culture);
                                    double BbAvAHA = double.Parse(specificList[17], culture);
                                    double PSCH = double.Parse(specificList[18], culture);
                                    double PSCD = double.Parse(specificList[19], culture);
                                    double PSCA = double.Parse(specificList[20], culture);

                                    var input = $"insert into specificbettingodds(matchId,bB1X2,BbMxH,BbAvH,BbMxD,BbAvD,BbMxA,BbAvA,BbOU,BbMxGT25,BbAvGT25,BbMxLT25,BbAvLT25,BbAH,BbAHH,BbMxAHH,BbAvAHH,BbMxAHA,BbAvAHA,PSCH,PSCD,PSCA,awayTeamId,homeTeamId,matchDate)" +
                                        $"values (@matchId,@bB1x2,@BbMxH,@BbAvH,@BbMxD,@BbAvD,@BbMxA,@BbAvA,@BbOU,@BbMxGT25,@BbAvGT25,@BbMxLT25,@BbAvLT25,@BbAH,@BbAHH,@BbMxAHH,@BbAvAHH,@BbMxAHA,@BbAvAHA,@PSCH,@PSCD,@PSCA,@awayTeamId,@homeTeamId,'{MatchDate}')";

                                    using (var inputResult = new SqlCommand(input, con))
                                    {
                                        inputResult.Parameters.AddWithValue("@matchId", matchId);
                                        inputResult.Parameters.AddWithValue("@bB1x2", Convert.ToDouble(bB1X2));
                                        inputResult.Parameters.AddWithValue("@BbMxH", Convert.ToDouble(BbMxH));
                                        inputResult.Parameters.AddWithValue("@BbAvH", Convert.ToDouble(BbAvH));
                                        inputResult.Parameters.AddWithValue("@BbMxD", Convert.ToDouble(BbMxD));
                                        inputResult.Parameters.AddWithValue("@BbAvD", Convert.ToDouble(BbAvD));
                                        inputResult.Parameters.AddWithValue("@BbMxA", Convert.ToDouble(BbMxA));
                                        inputResult.Parameters.AddWithValue("@BbAvA", Convert.ToDouble(BbAvA));
                                        inputResult.Parameters.AddWithValue("@BbOU", Convert.ToDouble(BbOU));

                                        inputResult.Parameters.AddWithValue("@BbMxGT25", Convert.ToDouble(BbMxGT25));
                                        inputResult.Parameters.AddWithValue("@BbAvGT25", Convert.ToDouble(BbAvGT25));
                                        inputResult.Parameters.AddWithValue("@BbMxLT25", Convert.ToDouble(BbMxLT25));
                                        inputResult.Parameters.AddWithValue("@BbAvLT25", Convert.ToDouble(BbAvLT25));
                                        inputResult.Parameters.AddWithValue("@BbAH", Convert.ToDouble(BbAH));
                                        inputResult.Parameters.AddWithValue("@BbAHH", Convert.ToDouble(BbAHH));
                                        inputResult.Parameters.AddWithValue("@BbMxAHH", Convert.ToDouble(BbMxAHH));
                                        inputResult.Parameters.AddWithValue("@BbAvAHH", Convert.ToDouble(BbAvAHH));
                                        inputResult.Parameters.AddWithValue("@BbMxAHA", Convert.ToDouble(BbMxAHA));
                                        inputResult.Parameters.AddWithValue("@BbAvAHA", Convert.ToDouble(BbAvAHA));
                                        inputResult.Parameters.AddWithValue("@PSCH", Convert.ToDouble(PSCH));
                                        inputResult.Parameters.AddWithValue("@PSCD", Convert.ToDouble(PSCD));
                                        inputResult.Parameters.AddWithValue("@PSCA", Convert.ToDouble(PSCA));
                                        inputResult.Parameters.AddWithValue("@awayTeamId", Convert.ToDouble(awayTeamId));
                                        inputResult.Parameters.AddWithValue("@homeTeamId", Convert.ToDouble(homeTeamId));

                                        inputResult.ExecuteNonQuery();
                                        Console.WriteLine("Record Pushed to database!");
                                        specificList.Clear();
                                    }
                                }

                            }
                        }
                    }
                    counter++;
                }
            }
        }


        


        private void CheckCountry(int counter, string[] values)
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
        }
        private static void LookUpAndInsertData(SqlConnection con, string query, string insert)
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
        private static DateOnly ConvertDate(string[] values)
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
        private static int IdForLeague(string div, int countryId, int leagueId, SqlConnection con)
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
        private static void StartIndexesForBettingCompanies(out int bet365Index, out int betAndWinIndex, out int interwettenIndex, out int ladbrokersIndex, out int pinnacleIndex, out int williamHillIndex, out int VCIndex, string[] values)
        {
            int firstIndex = Array.IndexOf(values, "B365H");
            bet365Index = firstIndex;
            Console.WriteLine("Bet365 Index found");

            int secondIndex = Array.IndexOf(values, "BWH");
            betAndWinIndex = secondIndex;
            Console.WriteLine("Bet & win Index found");

            int thirdIndex = Array.IndexOf(values, "IWH");
            interwettenIndex = thirdIndex;
            Console.WriteLine("Interwetten Index found");

            int fourthIndex = Array.IndexOf(values, "LBH");
            ladbrokersIndex = fourthIndex;
            Console.WriteLine("Ladbrokers Index found");

            int fifthIndex = Array.IndexOf(values, "PSH");
            pinnacleIndex = fifthIndex;
            Console.WriteLine("Pinnacle Index found");

            int sixthIndex = Array.IndexOf(values, "WHH");
            williamHillIndex = sixthIndex;
            Console.WriteLine("William Hill Index found");

            int seventhIndex = Array.IndexOf(values, "VCH");
            VCIndex = seventhIndex;
            Console.WriteLine("VC Bet Index found");
        }
    }
}

