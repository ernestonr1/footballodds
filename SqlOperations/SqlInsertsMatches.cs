using System.Data.SqlClient;
using System.Globalization;

namespace SqlOperations
{
    public class SqlInsertsMatches : SqlInserts
    {
        public SqlInsertsMatches(string filePath, string databaseConnectionString, bool isEnglish) : base(filePath, databaseConnectionString, isEnglish)
        {
        }



        public void PushDataForMatchesToDatabase()
        {
            string message = PushDataToMatches();
            PushDataToBettingCompanies();
            string message2 = PushMatchOdds();




            Console.WriteLine(message);
            Console.WriteLine(message2);

        }


        private void PushDataToBettingCompanies()
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

        private string PushMatchOdds()
        {

            int duplicatesFound = 0;
            int pushedNullValue = 0;
            int pushedToDb = 0;

            CultureInfo culture = CultureInfo.InvariantCulture;
            string homeTeam = "";
            string awayTeam = "";
            int homeTeamId = 0;
            int awayTeamId = 0;
            int homeTeamIndex = 0;
            int awayTeamIndex = 0;
            int matchId = 0;
            DateOnly matchDate;
            string? nullValue = "";

            int bet365Id = 0;
            int blueSquareId = 0;
            int betAndWinId = 0;
            int gameBookersId = 0;
            int interwettenId = 0;
            int ladbrokersId = 0;
            int pinnacleId = 0;
            int sportingOddsId = 0;
            int sportingBetId = 0;
            int stanJamesId = 0;
            int stanleyBetId = 0;
            int williamHillId = 0;
            int VCBetId = 0;


            int bet365Index = 0;
            int blueSquareIndex = 0;
            int betAndWinIndex = 0;
            int gameBookersIndex = 0;
            int interwettenIndex = 0;
            int ladbrokersIndex = 0;
            int pinnacleIndex = 0;
            int sportingOddsIndex = 0;
            int sportingBetIndex = 0;
            int stanJamesIndex = 0;
            int stanleyBetIndex = 0;
            int VCIndex = 0;
            int williamHillIndex = 0;






            decimal interhomeWin = 0M;
            decimal interdraw = 0M;
            decimal interawayWin = 0M;

            decimal whhomeWin = 0M;
            decimal whdraw = 0M;
            decimal whawayWin = 0M;

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

                            homeTeamIndex = Array.IndexOf(values, "HomeTeam");
                            awayTeamIndex = Array.IndexOf(values, "AwayTeam");




                            if (values.Contains("Referee"))
                            {
                                IsEnglish = true;
                            }
                            else
                            {
                                IsEnglish = false;
                            }
                            StartIndexesForBettingCompanies(out bet365Index, out blueSquareIndex, out betAndWinIndex, out gameBookersIndex,
                                out interwettenIndex, out ladbrokersIndex, out pinnacleIndex, out sportingOddsIndex, out sportingBetIndex,
                                out stanJamesIndex, out stanleyBetIndex, out williamHillIndex, out VCIndex, values);
                        }

                        if (counter > 0 && !reader.EndOfStream)
                        {
                            if (values[1].Length <= 8)
                            {
                                matchDate = ConvertDate(values);
                            }
                            else
                            {
                                matchDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);
                            }
                            homeTeam = values[homeTeamIndex];
                            awayTeam = values[awayTeamIndex];

                            //find homeTeamId
                            var homeTeamIdCheck = $"select id from Teams where teamname = @teamName";
                            homeTeamId = CheckOnTeam(homeTeam, homeTeamId, con, homeTeamIdCheck);
                            //find awayTeamId
                            var awayTeamIdCheck = $"select id from Teams where teamname = @teamName";
                            awayTeamId = CheckOnTeam(awayTeam, awayTeamId, con, awayTeamIdCheck);

                            var matchIdCheck = $"select id from Matches where awayTeamId = ('{awayTeamId}') and homeTeamId = ('{homeTeamId}') and matchDate = ('{matchDate}')";
                            using (var match = new SqlCommand(matchIdCheck, con))
                            {
                                int doubleExist = (int)match.ExecuteScalar();
                                if (doubleExist > 0)
                                {
                                    matchId = doubleExist;
                                }
                            }










                            var interwettenSelect = $"select id from bettingcompanies where BettingCompanyName = ('Interwetten')";
                            using (var interwetten = new SqlCommand(interwettenSelect, con))
                            {

                                if (interwettenIndex == -1 || values[interwettenIndex] == "" || values[interwettenIndex + 1] == "" || values[interwettenIndex + 2] == "")
                                {
                                    nullValue = null;
                                }
                                else if (williamHillIndex == -1 || values[williamHillIndex] == "" || values[williamHillIndex + 1] == "" || values[williamHillIndex + 2] == "")
                                {
                                    nullValue = null;
                                }
                                else
                                {
                                    var interwettenhomeRes = decimal.TryParse(values[interwettenIndex].Replace(".", ","), out interhomeWin);
                                    var interdrawRes = decimal.TryParse(values[interwettenIndex + 1].Replace(".", ","), out interdraw);
                                    var interawayWinRes = decimal.TryParse(values[interwettenIndex + 2].Replace(".", ","), out interawayWin);

                                    var whhomeWinRes = decimal.TryParse(values[williamHillIndex].Replace(".", ","), out whhomeWin);
                                    var whdrawRes = decimal.TryParse(values[williamHillIndex + 1].Replace(".", ","), out whdraw);
                                    var whawayWinRes = decimal.TryParse(values[williamHillIndex + 2].Replace(".", ","), out whawayWin);
                                }
                                int doubleExistInterwetten = (int)interwetten.ExecuteScalar();

                                if (doubleExistInterwetten > 0)
                                {
                                    interwettenId = doubleExistInterwetten;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId";

                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);


                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            duplicatesFound++;
                                        }
                                        else if (checkMatchId == 0)
                                        {

                                            var input = $"insert into matchodds(matchid,bettingcompanyid,interwettenhometeamwinodds,interwettendrawteamwinodds,interwettenawayteamwinodds,williamhillhometeamwinodds,williamhilldrawteamwinodds,williamhillawayteamwinodds) values (@matchId,@betId,@interhomeWin,@interdraw,@interawayWin,@whhomeWin,@whdraw,@whawayWin)";
                                            using (var inputResult = new SqlCommand(input, con))
                                            {
                                                if (interwettenIndex == -1 && values[interwettenIndex] == "" && williamHillIndex != -1 && values[williamHillIndex] != "")
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@betId", DBNull.Value);

                                                    inputResult.Parameters.AddWithValue("@interhomeWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@interdraw", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@interawayWin", DBNull.Value);

                                                    inputResult.Parameters.AddWithValue("@whhomeWin", whhomeWin);
                                                    inputResult.Parameters.AddWithValue("@whdraw", whdraw);
                                                    inputResult.Parameters.AddWithValue("@whawayWin", whawayWin);


                                                    inputResult.ExecuteNonQuery();

                                                }

                                                else if (interwettenIndex != -1 && values[interwettenIndex] != "" && williamHillIndex == -1 && values[williamHillIndex] == "")
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@betId", DBNull.Value);

                                                    inputResult.Parameters.AddWithValue("@interhomeWin", interhomeWin);
                                                    inputResult.Parameters.AddWithValue("@interdraw", interdraw);
                                                    inputResult.Parameters.AddWithValue("@interawayWin", interawayWin);

                                                    inputResult.Parameters.AddWithValue("@whhomeWin", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@whdraw", DBNull.Value);
                                                    inputResult.Parameters.AddWithValue("@whawayWin", DBNull.Value);


                                                    inputResult.ExecuteNonQuery();

                                                }

                                                else
                                                {
                                                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                                                    inputResult.Parameters.AddWithValue("@betId", DBNull.Value);

                                                    inputResult.Parameters.AddWithValue("@interhomeWin", interhomeWin);
                                                    inputResult.Parameters.AddWithValue("@interdraw", interdraw);
                                                    inputResult.Parameters.AddWithValue("@interawayWin", interawayWin);

                                                    inputResult.Parameters.AddWithValue("@whhomeWin", whhomeWin);
                                                    inputResult.Parameters.AddWithValue("@whdraw", whdraw);
                                                    inputResult.Parameters.AddWithValue("@whawayWin", whawayWin);


                                                    inputResult.ExecuteNonQuery();
                                                }


                                            }
                                            // InsertIntoMatchOdds(matchId, interwettenId, interwettenIndex, homeWin, draw, awayWin, con, values, input, ref pushedNullValue, ref pushedToDb);
                                        }
                                    }
                                }
                            }

                            matchOdds.Clear();
                        }
                        counter++;
                    }
                    reader.Close();
                }
            }
            var message = $"MatchData\nDouble records found: {duplicatesFound}, null value pushed: {pushedNullValue}, record pushed to db: {pushedToDb}";
            return message;
        }

        private static void InsertIntoMatchOdds(int matchId, int betId, int betIndex, decimal homeWin, decimal draw, decimal awayWin, SqlConnection con, string[] values, string input, ref int nullValue, ref int pushedToDatabase)
        {
            using (var inputResult = new SqlCommand(input, con))
            {
                if (betIndex == -1 || values[betIndex] == "")
                {
                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                    inputResult.Parameters.AddWithValue("@betId", betId);
                    inputResult.Parameters.AddWithValue("@homeWin", DBNull.Value);
                    inputResult.Parameters.AddWithValue("@draw", DBNull.Value);
                    inputResult.Parameters.AddWithValue("@awayWin", DBNull.Value);


                    inputResult.ExecuteNonQuery();
                    nullValue++;
                }
                else if (betIndex != -1)
                {
                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                    inputResult.Parameters.AddWithValue("@betId", betId);
                    inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                    inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                    inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));



                    inputResult.ExecuteNonQuery();
                    pushedToDatabase++;
                }
            }
        }

        private string PushDataToMatches()
        {

            int pushedToDB = 0;
            int pushedNullToDb = 0;
            int foundDoubles = 0;
            int attendenceIndex = 0;


            var homeTeamIndex = 0;
            var awayTeamIndex = 0;
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
            double Attendence = 0;

            using (StreamReader reader = new StreamReader($"{FilePath}"))
            {
                string[] values;
                int counter = 0;
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    values = line.Split(',', ';');
                    if (counter == 0)
                    {


                        homeTeamIndex = Array.IndexOf(values, "HomeTeam");
                        awayTeamIndex = Array.IndexOf(values, "AwayTeam");

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
                        FTHGIndex = LookUpIndex(values, "FTHG", "FTHG");
                        HTRIndex = LookUpIndex(values, "HTR", "HTR");
                        HSIndex = LookUpIndex(values, "HS", "FTHG");
                        ARIndex = LookUpIndex(values, "AR", "AR");
                        attendenceIndex = LookUpIndex(values, "Attendance", "Attendance");
                    }

                    if (counter == 1)
                    {
                        if (values[1].Length <= 8)
                            StartDate = ConvertDate(values);
                        else
                            StartDate = DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture);

                        div = values[0].ToString();
                    }

                    if (counter > 0 && !reader.EndOfStream)
                    {
                        if (values[1].Length <= 8)
                            MatchDate = ConvertDate(values);
                        else
                            MatchDate = (DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture));

                        HomeTeam = (values[homeTeamIndex]);
                        AwayTeam = (values[awayTeamIndex]);
                        using (var con = new SqlConnection($"{DatabaseConnectionString}"))
                        {
                            con.Open();


                            //check home team
                            var checkOnHomeTeam = $"select id from teams where teamname = @teamName";
                            homeTeamId = CheckOnTeam(HomeTeam, homeTeamId, con, checkOnHomeTeam);
                            //check away team
                            var checkOnAwayTeam = $"select id from teams where teamname = @teamName";
                            awayTeamId = CheckOnTeam(AwayTeam, awayTeamId, con, checkOnAwayTeam);
                            //check country id
                            var checkCountry = $"select id from countries where name = @teamName";
                            countryId = CheckOnTeam(country, countryId, con, checkCountry);


                            leagueId = IdForLeague(div, countryId, leagueId, con);
                            var checkSeason = $"select id from seasons where startdate = ('{StartDate}') and leagueId = ('{leagueId}')";
                            using (var checkOnSeason = new SqlCommand(checkSeason, con))
                            {
                                int doubleExist = (int)checkOnSeason.ExecuteScalar();
                                if (doubleExist > 0)
                                    seasonId = doubleExist;
                            }

                            var tryConvert = int.TryParse(values[HSIndex], out int res);

                            if (country == "England")
                            {
                                FTHG = Convert.ToInt32(values[FTHGIndex]);
                                FTAG = Convert.ToInt32(values[FTHGIndex + 1]);
                                FTR = values[FTHGIndex + 2];
                                HTHG = Convert.ToInt32(values[FTHGIndex + 3]);
                                HTAG = Convert.ToInt32(values[FTHGIndex + 4]);
                                HTR = values[HTRIndex];


                                if (attendenceIndex != -1)
                                {
                                    Referee = values[HTRIndex + 2];
                                    HS = Convert.ToInt32(values[HSIndex + 1]);
                                }
                                else
                                {
                                    Referee = values[HTRIndex + 1];
                                    HS = Convert.ToInt32(values[HSIndex]);
                                }

                                if (values[HTRIndex + 2].Contains("\""))
                                {
                                    Referee = values[HTRIndex + 2] + values[HTRIndex + 3];
                                    Referee = Referee.Replace(@"""", String.Empty);
                                }



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

                            if (attendenceIndex != -1)
                            {
                                var tryme = double.TryParse(values[attendenceIndex], out Attendence);
                            }

                            var checkInsertOperation = $"select count(*) from Matches where homeTeamId = ('{homeTeamId}') and awayTeamId = ('{awayTeamId}') and seasonId = ('{seasonId}')" +
                                $" and  matchdate = ('{MatchDate}')";

                            var insertOperationGermany = $"insert into Matches(homeTeamId,awayTeamId,seasonId,matchDate,FTHG,FTAG,FTR,HTHG,HTAG,HTR,attendence,REFEREE,HS,[AS],HST,AST,HF,AF,HC,AC,HY,AY,HR,AR) " +
                                $"values (@homeTeamId,@awayTeamId,@seasonId,'{MatchDate}',@FTHG,@FTAG,@FTR,@HTHG,@HTAG,@HTR,@attendence,@Referee,@HS,@AS,@HST,@AST,@HF,@AF,@HC,@AC,@HY,@AY,@HR,@AR)";

                            if (country == "England")
                            {
                                using (var englanResult = new SqlCommand(checkInsertOperation, con))
                                {
                                    var rowExist = (int)englanResult.ExecuteScalar();
                                    if (rowExist > 0)
                                    {
                                        foundDoubles++;

                                    }
                                    else if (rowExist == 0)
                                    {
                                        if (attendenceIndex != -1)
                                        {
                                            using (var inputResult = new SqlCommand(checkInsertOperation, con))
                                            {
                                                var rowExistHere = (int)inputResult.ExecuteScalar();
                                                if (rowExistHere > 0)
                                                {
                                                    foundDoubles++;
                                                }
                                                else if (rowExistHere == 0)
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
                                                        insertCommand.Parameters.AddWithValue("@attendence", Attendence);
                                                        insertCommand.Parameters.AddWithValue("@Referee", Referee);
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

                                                        pushedToDB++;
                                                    }
                                                }
                                            }
                                        }
                                        else
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
                                                insertCommand.Parameters.AddWithValue("@attendence", DBNull.Value);
                                                insertCommand.Parameters.AddWithValue("@Referee", Referee);
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

                                                pushedToDB++;
                                            }
                                        }
                                    }
                                }

                            }



                            if (country != "England")
                            {
                                using (var inputResult = new SqlCommand(checkInsertOperation, con))
                                {
                                    var rowExist = (int)inputResult.ExecuteScalar();
                                    if (rowExist > 0)
                                    {
                                        foundDoubles++;
                                    }
                                    else if (rowExist == 0)
                                    {
                                        if (attendenceIndex != -1)
                                        {
                                            using (var inputResultGermany = new SqlCommand(checkInsertOperation, con))
                                            {
                                                var rowExistHere = (int)inputResult.ExecuteScalar();
                                                if (rowExistHere > 0)
                                                {
                                                    foundDoubles++;
                                                }
                                                else if (rowExistHere == 0)
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
                                                        insertCommand.Parameters.AddWithValue("@attendence", Attendence);
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

                                                        pushedToDB++;
                                                    }
                                                }
                                            }
                                        }
                                        else
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
                                                insertCommand.Parameters.AddWithValue("@attendence", DBNull.Value);
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

                                                pushedToDB++;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    counter++;
                }
            }
            string message;
            message = $"Found {foundDoubles} double records and pushed {pushedToDB} records";
            return message;

        }

        private static int CheckOnTeam(string HomeTeam, int homeTeamId, SqlConnection con, string checkOnHomeTeam)
        {
            using (var checkOnTeam = new SqlCommand(checkOnHomeTeam, con))
            {
                checkOnTeam.Parameters.AddWithValue("@teamName", HomeTeam);
                int doubleExists = (int)checkOnTeam.ExecuteScalar();
                if (doubleExists > 0)
                    homeTeamId = doubleExists;
            }

            return homeTeamId;
        }

        private static void StartIndexesForBettingCompanies(out int bet365Index, out int blueSquareIndex, out int betAndWinIndex,
              out int gameBookersIndex, out int interwettenIndex, out int ladbrokersIndex, out int pinnacleIndex,
              out int sportingOddsIndex, out int sportingBetIndex, out int stanJamesIndex, out int stanleyBetIndex,
              out int williamHillIndex, out int VCIndex, string[] values)
        {
            bet365Index = LookUpIndex(values, "B365H", "Bet 365");
            betAndWinIndex = LookUpIndex(values, "BWH", "Bet and win");
            blueSquareIndex = LookUpIndex(values, "BSH", "Blue Square");
            gameBookersIndex = LookUpIndex(values, "GBH", "Gamebookers");
            interwettenIndex = LookUpIndex(values, "IWH", "Interwetten");
            ladbrokersIndex = LookUpIndex(values, "LBH", "Ladbrookers");
            pinnacleIndex = LookUpIndex(values, "PSH", "Pinnacle");
            sportingOddsIndex = LookUpIndex(values, "SOH", "SportingOdds");
            sportingBetIndex = LookUpIndex(values, "SBH", "Sporting Bet");
            stanJamesIndex = LookUpIndex(values, "SJH", "Stan James");
            stanleyBetIndex = LookUpIndex(values, "SYH", "Stanley Bet");
            williamHillIndex = LookUpIndex(values, "WHH", "William Hill");
            VCIndex = LookUpIndex(values, "VCH", "VC");
        }

        private static int LookUpIndex(string[] values, string inputCompanyH, string inputCompanyName)
        {
            int bettingCompanyIndex;
            int firstIndex = Array.IndexOf(values, inputCompanyH);
            bettingCompanyIndex = firstIndex;
            Console.WriteLine($"{inputCompanyName} Index found");
            return bettingCompanyIndex;
        }
    }
}
