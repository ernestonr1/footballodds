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
                            country = "England";
                        else
                            country = "Germany";
                    }


                    if (counter == 1)
                    {
                        div = values[0].ToString();
                        StartDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                    }

                    if (counter > 0)
                    {
                        MatchDate = (DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture));
                        HomeTeam = (values[2]);
                        AwayTeam = (values[3]);

                        using (var con = new SqlConnection($"{DatabaseConnectionString}"))
                        {
                            con.Open();


                            //check home team
                            var checkOnHomeTeam = $"select id from teams where teamname = ('{HomeTeam}')";
                            using (var checkOnTeam = new SqlCommand(checkOnHomeTeam, con))
                            {
                                int doubleExists = (int)checkOnTeam.ExecuteScalar();
                                if (doubleExists > 0)
                                    homeTeamId = doubleExists;
                            }
                            //check away team
                            var checkOnAwayTeam = $"select id from teams where teamname = ('{AwayTeam}')";
                            using (var checkOnTeam = new SqlCommand(checkOnAwayTeam, con))
                            {
                                int doubleExists = (int)checkOnTeam.ExecuteScalar();
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
                                {
                                    seasonId = doubleExist;
                                }
                            }
                            if (country == "England")
                            {
                                FTHG = Convert.ToInt32(values[4]);
                                FTAG = Convert.ToInt32(values[5]);
                                FTR = values[6];
                                HTHG = Convert.ToInt32(values[7]);
                                HTAG = Convert.ToInt32(values[8]);
                                HTR = values[9];
                                Referee = values[10];
                                HS = Convert.ToInt32(values[11]);
                                AS = Convert.ToInt32(values[12]);
                                HST = Convert.ToInt32(values[13]);
                                AST = Convert.ToInt32(values[14]);
                                HF = Convert.ToInt32(values[15]);
                                AF = Convert.ToInt32(values[16]);
                                HC = Convert.ToInt32(values[17]);
                                AC = Convert.ToInt32(values[18]);
                                HY = Convert.ToInt32(values[19]);
                                AY = Convert.ToInt32(values[20]);
                                HR = Convert.ToInt32(values[21]);
                                AR = Convert.ToInt32(values[22]);
                            }
                            if (country == "Germany")
                            {
                                FTHG = Convert.ToInt32(values[4]);
                                FTAG = Convert.ToInt32(values[5]);
                                FTR = values[6];
                                HTHG = Convert.ToInt32(values[7]);
                                HTAG = Convert.ToInt32(values[8]);
                                HTR = values[9];
                                Referee = "Null";
                                HS = Convert.ToInt32(values[10]);
                                AS = Convert.ToInt32(values[11]);
                                HST = Convert.ToInt32(values[12]);
                                AST = Convert.ToInt32(values[13]);
                                HF = Convert.ToInt32(values[14]);
                                AF = Convert.ToInt32(values[15]);
                                HC = Convert.ToInt32(values[16]);
                                AC = Convert.ToInt32(values[17]);
                                HY = Convert.ToInt32(values[18]);
                                AY = Convert.ToInt32(values[19]);
                                HR = Convert.ToInt32(values[20]);
                                AR = Convert.ToInt32(values[21]);
                            }
                            //var insertOperationOne = $"insert into Matches(homeTeamId,awayTeamId,seasonId,matchDate,FTHG,FTAG,FTR,HTHG,HTAG,HTR,REFEREE) values ('{homeTeamId}','{awayTeamId}','{seasonId}','{MatchDate}','{FTHG}','{FTAG}','{FTR}','{HTHG}','{HTAG}','{HTR}','{Referee}'}')";

                            var checkInsertOperation = $"select count(*) from Matches where homeTeamId = ('{homeTeamId}') and awayTeamId = ('{awayTeamId}') and seasonId = ('{seasonId}')" +
                                $" and  matchdate = ('{MatchDate}') and fthg = ('{FTHG}') and ftag = ('{FTAG}') and ftr = ('{FTR}') and hthg = ('{HTHG}')" +
                                $" and htag = ('{HTAG}') and htr = ('{HTR}') and referee = ('{Referee}') and hs = ('{HS}') and [as] = ('{AS}') and hst = ('{HST}') and ast = ('{AST}')" +
                                $"and hf = ('{HF}') and af = ('{AF}') and hc = ('{HC}') and ac = ('{AC}') and hy = ('{HY}') and ay = ('{AY}') and hr = ('{HR}') and ar = ('{AR}')";
                            using (var checkInsert = new SqlCommand(checkInsertOperation, con))
                            {
                                int doubleChecked = (int)checkInsert.ExecuteScalar();
                                if (doubleChecked > 0)
                                {
                                    Console.WriteLine("Duplicates found!");
                                }
                                else if (doubleChecked == 0)
                                {

                                    var insertOperation = $"insert into Matches(homeTeamId,awayTeamId,seasonId,matchDate,FTHG,FTAG,FTR,HTHG,HTAG,HTR,REFEREE,HS,[AS],HST,AST,HF,AF,HC,AC,HY,AY,HR,AR) values ('{homeTeamId}','{awayTeamId}','{seasonId}','{MatchDate}','{FTHG}','{FTAG}','{FTR}','{HTHG}','{HTAG}','{HTR}','{Referee}','{HS}','{AS}','{HST}','{AST}','{HF}','{AF}','{HC}','{AC}','{HY}','{AY}','{HR}','{AR}')";
                                    using (var InsertCommand = new SqlCommand(insertOperation, con))
                                    {
                                        InsertCommand.ExecuteNonQuery();
                                        Console.WriteLine("Record Pushed to database!");

                                    }
                                }
                            }



                        }



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

            int bet365Id = 0;
            int betAndWinId = 0;
            int interwettenId = 0;
            int ladbrokersId = 0;
            int pinnacleId = 0;
            int williamHillId = 0;
            int VCBetId = 0;

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


                        if (counter > 0)
                        {
                            matchDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                            homeTeam = values[2];
                            awayTeam = values[3];

                            var homeTeamIdCheck = $"select id from Teams where teamname = ('{homeTeam}')";
                            using (var home = new SqlCommand(homeTeamIdCheck, con))
                            {

                                int doubleExists = (int)home.ExecuteScalar();
                                if (doubleExists > 0)
                                {
                                    homeTeamId = doubleExists;
                                }
                            }

                            var awayTeamIdCheck = $"select id from Teams where teamname = ('{awayTeam}')";
                            using (var away = new SqlCommand(awayTeamIdCheck, con))
                            {
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

                                    if (IsEnglish)
                                        for (int i = 23; i <= 43; i++)
                                        {
                                            matchOdds.Add((values[i].ToString()));
                                        }
                                    else
                                    {
                                        for (int i = 22; i <= 42; i++)
                                        {
                                            matchOdds.Add((values[i].ToString()));
                                        }
                                    }

                                    var bet365Select = $"select id from bettingcompanies where BettingCompanyName = ('Bet365')";
                                    using (var bet365 = new SqlCommand(bet365Select, con))
                                    {
                                        decimal home = decimal.Parse(matchOdds[0], culture);
                                        decimal draw1 = Convert.ToDecimal(matchOdds[1], culture);
                                        decimal away = Convert.ToDecimal(matchOdds[2], culture);

                                        int doubleExist365 = (int)bet365.ExecuteScalar();
                                        if (doubleExist365 > 0)
                                        {
                                            bet365Id = doubleExist365;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) values (@matchId,@bet365Id,@home,@draw1,@away,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {
                                                inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                inputResult.Parameters.AddWithValue("@bet365Id", bet365Id);
                                                inputResult.Parameters.AddWithValue("@home", Convert.ToDecimal(home));
                                                inputResult.Parameters.AddWithValue("@draw1", Convert.ToDecimal(draw1));
                                                inputResult.Parameters.AddWithValue("@away", Convert.ToDecimal(away));
                                                inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                inputResult.ExecuteNonQuery();
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }

                                    var betandwinSelect = $"select id from bettingcompanies where BettingCompanyName = ('Bet&Win')";
                                    using (var betandwin = new SqlCommand(betandwinSelect, con))
                                    {

                                        homeWin = decimal.Parse(matchOdds[3], culture);
                                        draw = decimal.Parse(matchOdds[4], culture);
                                        awayWin = decimal.Parse(matchOdds[5], culture);

                                        int doubleExistBetAndWin = (int)betandwin.ExecuteScalar();
                                        if (doubleExistBetAndWin > 0)
                                        {
                                            betAndWinId = doubleExistBetAndWin;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@betAndWinId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {


                                                inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                inputResult.Parameters.AddWithValue("@betAndWinId", betAndWinId);
                                                inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);

                                                inputResult.ExecuteNonQuery();
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }

                                    var interwettenSelect = $"select id from bettingcompanies where BettingCompanyName = ('Interwetten')";
                                    using (var interwetten = new SqlCommand(interwettenSelect, con))
                                    {

                                        homeWin = decimal.Parse(matchOdds[6], culture);
                                        draw = decimal.Parse(matchOdds[7], culture);
                                        awayWin = decimal.Parse(matchOdds[8], culture);

                                        int doubleExistInterwetten = (int)interwetten.ExecuteScalar();
                                        if (doubleExistInterwetten > 0)
                                        {
                                            interwettenId = doubleExistInterwetten;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@interwettenId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {

                                                inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                inputResult.Parameters.AddWithValue("@interwettenId", interwettenId);
                                                inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);


                                                inputResult.ExecuteNonQuery();
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }

                                    var ladbrokersSelect = $"select id from bettingcompanies where BettingCompanyName = ('Ladbrokes')";
                                    using (var ladbrokers = new SqlCommand(ladbrokersSelect, con))
                                    {

                                        homeWin = decimal.Parse(matchOdds[9], culture);
                                        draw = decimal.Parse(matchOdds[10], culture);
                                        awayWin = decimal.Parse(matchOdds[11], culture);

                                        int doubleExistLadbrokers = (int)ladbrokers.ExecuteScalar();
                                        if (doubleExistLadbrokers > 0)
                                        {
                                            ladbrokersId = doubleExistLadbrokers;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@ladbrokersId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {

                                                inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                inputResult.Parameters.AddWithValue("@ladbrokersId", ladbrokersId);
                                                inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);

                                                inputResult.ExecuteNonQuery();
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }

                                    var pinnacleSelect = $"select id from bettingcompanies where BettingCompanyName = ('Pinnacle')";
                                    using (var pinnacle = new SqlCommand(pinnacleSelect, con))
                                    {

                                        homeWin = decimal.Parse(matchOdds[12], culture);
                                        draw = decimal.Parse(matchOdds[13], culture);
                                        awayWin = decimal.Parse(matchOdds[14], culture);

                                        int doubleExistPinnacle = (int)pinnacle.ExecuteScalar();
                                        if (doubleExistPinnacle > 0)
                                        {
                                            pinnacleId = doubleExistPinnacle;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@pinnacleId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {

                                                inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                inputResult.Parameters.AddWithValue("@pinnacleId", pinnacleId);
                                                inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);
                                                inputResult.ExecuteNonQuery();
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }
                                    var williamHillSelect = $"select id from bettingcompanies where BettingCompanyName = ('William Hill')";
                                    using (var williamHill = new SqlCommand(williamHillSelect, con))
                                    {
                                        int doubleExistWilliamHill = (int)williamHill.ExecuteScalar();
                                        if (doubleExistWilliamHill > 0)
                                        {
                                            homeWin = decimal.Parse(matchOdds[15], culture);
                                            draw = decimal.Parse(matchOdds[16], culture);
                                            awayWin = decimal.Parse(matchOdds[17], culture);

                                            williamHillId = doubleExistWilliamHill;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@williamHillId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {

                                                inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                inputResult.Parameters.AddWithValue("@williamHillId", williamHillId);
                                                inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);

                                                inputResult.ExecuteNonQuery();
                                                Console.WriteLine("Record Pushed to database!");
                                            }
                                        }
                                    }

                                    var VCBetSelect = $"select id from bettingcompanies where BettingCompanyName = ('VC Bet')";
                                    using (var VCBet = new SqlCommand(VCBetSelect, con))
                                    {


                                        homeWin = decimal.Parse(matchOdds[18], culture);
                                        draw = decimal.Parse(matchOdds[19], culture);
                                        awayWin = decimal.Parse(matchOdds[20], culture);

                                        int doubleExistVCBet = (int)VCBet.ExecuteScalar();
                                        if (doubleExistVCBet > 0)
                                        {
                                            VCBetId = doubleExistVCBet;
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,awayteamid,hometeamid,matchdate) " +
                                                $"values (@matchId,@VCBetId,@homeWin,@draw,@awayWin,@awayTeamId,@homeTeamId,'{matchDate}')";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {
                                                inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                inputResult.Parameters.AddWithValue("@VCBetId", VCBetId);
                                                inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                                                inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                                                inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));
                                                inputResult.Parameters.AddWithValue("@awayTeamId", awayTeamId);
                                                inputResult.Parameters.AddWithValue("@homeTeamId", homeTeamId);


                                                inputResult.ExecuteNonQuery();
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

                //retrieve home and away teams ids
                //retrieve matchId with away & home team ids + date
                //chech for doubles with home & awayteam ids + date + matchId

                //using(var cmd = con.CreateCommand())

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
            CultureInfo culture = CultureInfo.InvariantCulture;
            List<string> specificList = new List<string>();

            using (StreamReader reader = new StreamReader($"{FilePath}"))
            {
                int counter = 0;
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(';', ',');
                    if (counter > 0)
                    {
                        if (IsEnglish)
                        {
                            //column 44 -64
                            for (int i = 44; i <= 64; i++)
                            {
                                specificList.Add(values[i].ToString());
                            }
                        }
                        else
                        {
                            for (int i = 43; i <= 63; i++)
                            {
                                specificList.Add(values[i].ToString());
                            }
                            //column 43-63
                        }

                        MatchDate = (DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture));
                        HomeTeam = (values[2]);
                        AwayTeam = (values[3]);

                        using (var con = new SqlConnection($"{DatabaseConnectionString}"))
                        {
                            con.Open();
                            var homeTeamCheck = $"select id from teams where teamname = ('{HomeTeam}')";
                            using (var first = new SqlCommand(homeTeamCheck, con))
                            {
                                int doubleValues = (int)first.ExecuteScalar();
                                if (doubleValues > 0)
                                    homeTeamId = doubleValues;

                            }
                            var awayTeamCheck = $"select id from teams where teamname = ('{AwayTeam}')";
                            using (var first = new SqlCommand(awayTeamCheck, con))
                            {
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

                            var doubleRecord = $"select count(*) from specificbettingodds where homeTeamId = ('{homeTeamId}') and awayTeamId = ('{awayTeamId}') and mathDate = ('{MatchDate}') and id = ('{matchId}')";
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




                                    var input = $"insert into specificbettingodds(matchId,bB1X2,BbMxH,BbAvH,BbMxD,BbAvD,BbMxA,BbAvA,BbOU,BbMxGT25,BbAvGT25,BbMxLT25,BbAvLT25,BbAH,BbAHH,BbMxAHH,BbAvAHH,BbMxAHA,BbAvAHA,PSCH,PSCD,PSCA,awayTeamId,homeTeamId,mathDate)" +
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
    }
}

