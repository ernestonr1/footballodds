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
            //PushDataToMatches();
            //PushDataToBettingCompanies();

            //PushMatchOdds();
            PushSpecificMatchData();
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

        void PushMatchOdds()
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




            int closingHomeBet365 = 0;
            int closingDrawBet365 = 0;
            int closingAwayBet365 = 0;


            int closingDrawBlueSquare = 0;
            int closingAwayBlueSquare = 0;
            int closingHomeBlueSquare = 0;


            int closingHomeBetAndWin = 0;
            int closingDrawBetAndWin = 0;
            int closingAwayBetAndWin = 0;


            int closingHomeGameBookers = 0;
            int closingDrawGameBookers = 0;
            int closingAwayGameBookers = 0;

            int closingHomeInterWetten = 0;
            int closingDrawInterWetten = 0;
            int closingAwayInterWetten = 0;

            int closingHomeLadbrookers = 0;
            int closingDrawLadbrookers = 0;
            int closingAwayLadbrookers = 0;

            int closingHomePinnacle = 0;
            int closingDrawPinnacle = 0;
            int closingAwayPinnacle = 0;


            int closingHomeSportingOdds = 0;
            int closingDrawSportingOdds = 0;
            int closingAwaySportingOdds = 0;

            int closingHomeSportingBet = 0;
            int closingDrawSportingBet = 0;
            int closingAwaySportingBet = 0;

            int closingHomeStanJames = 0;
            int closingDrawStanJames = 0;
            int closingAwayStanJames = 0;


            int closingHomeStanleybey = 0;
            int closingDrawStanleybey = 0;
            int closingAwayStanleybey = 0;


            int closingHomeVCBet = 0;
            int closingDrawVCBet = 0;
            int closingAwayVCBet = 0;


            int closingHomeWilliamHill = 0;
            int closingDrawWilliamHill = 0;
            int closingAwayWilliamHill = 0;

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
                            closingHomeBet365 = Array.IndexOf(values, "B365CH");
                            closingDrawBet365 = Array.IndexOf(values, "B365CD");
                            closingAwayBet365 = Array.IndexOf(values, "B365CA");

                            closingHomeBlueSquare = Array.IndexOf(values, "BSCH");
                            closingDrawBlueSquare = Array.IndexOf(values, "BSCD");
                            closingAwayBlueSquare = Array.IndexOf(values, "BSCA");

                            closingHomeBetAndWin = Array.IndexOf(values, "BWCH");
                            closingDrawBetAndWin = Array.IndexOf(values, "BWCD");
                            closingAwayBetAndWin = Array.IndexOf(values, "BWCA");

                            closingHomeGameBookers = Array.IndexOf(values, "GBCH");
                            closingDrawGameBookers = Array.IndexOf(values, "GBCD");
                            closingAwayGameBookers = Array.IndexOf(values, "GBCA");

                            closingHomeInterWetten = Array.IndexOf(values, "IWCH");
                            closingDrawInterWetten = Array.IndexOf(values, "IWCD");
                            closingAwayInterWetten = Array.IndexOf(values, "IWCA");

                            closingHomeLadbrookers = Array.IndexOf(values, "LBCH");
                            closingDrawLadbrookers = Array.IndexOf(values, "LBCD");
                            closingAwayLadbrookers = Array.IndexOf(values, "LBCA");

                            closingHomePinnacle = Array.IndexOf(values, "PSCH");
                            closingDrawPinnacle = Array.IndexOf(values, "PSCD");
                            closingAwayPinnacle = Array.IndexOf(values, "PSCA");

                            closingHomeSportingOdds = Array.IndexOf(values, "SOCH");
                            closingDrawSportingOdds = Array.IndexOf(values, "SOCD");
                            closingAwaySportingOdds = Array.IndexOf(values, "SOCA");

                            closingHomeSportingBet = Array.IndexOf(values, "SBCH");
                            closingDrawSportingBet = Array.IndexOf(values, "SBCD");
                            closingAwaySportingBet = Array.IndexOf(values, "SBCA");

                            closingHomeStanJames = Array.IndexOf(values, "SJCH");
                            closingDrawStanJames = Array.IndexOf(values, "SJCD");
                            closingAwayStanJames = Array.IndexOf(values, "SJCA");

                            closingHomeStanleybey = Array.IndexOf(values, "SYCH");
                            closingDrawStanleybey = Array.IndexOf(values, "SYCD");
                            closingAwayStanleybey = Array.IndexOf(values, "SYCA");

                            closingHomeVCBet = Array.IndexOf(values, "VCCH");
                            closingDrawVCBet = Array.IndexOf(values, "VCCD");
                            closingAwayVCBet = Array.IndexOf(values, "VCCA");

                            closingHomeWilliamHill = Array.IndexOf(values, "WHCH");
                            closingDrawWilliamHill = Array.IndexOf(values, "WHCD");
                            closingAwayWilliamHill = Array.IndexOf(values, "WHCA");




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

                            var bet365Select = $"select id from bettingcompanies where BettingCompanyName = ('Bet365')";
                            using (var bet365 = new SqlCommand(bet365Select, con))
                            {

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


                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", bet365Id);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, bet365Id, bet365Index, homeWin, draw, awayWin, con, values, input, closingHomeBet365, closingDrawBet365, closingAwayBet365);
                                        }
                                    }
                                }
                            }


                            var blueSquareSelect = $"select id from bettingcompanies where BettingCompanyName = ('Blue Square')";
                            using (var bluewSquare = new SqlCommand(blueSquareSelect, con))
                            {
                                if (blueSquareIndex == -1 || values[blueSquareIndex] == "")
                                {
                                    nullValue = null;
                                }
                                else
                                {
                                    homeWin = decimal.Parse(values[blueSquareIndex], culture);
                                    draw = decimal.Parse(values[blueSquareIndex + 1], culture);
                                    awayWin = decimal.Parse(values[blueSquareIndex + 2], culture);
                                }

                                int doubleExistBlueSquare = (int)bluewSquare.ExecuteScalar();
                                if (doubleExistBlueSquare > 0)
                                {
                                    blueSquareId = doubleExistBlueSquare;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", blueSquareId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, blueSquareId, blueSquareIndex, homeWin, draw, awayWin, con, values, input, closingHomeBlueSquare, closingDrawBlueSquare, closingAwayBlueSquare);
                                        }
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

                                int doubleExistBetAndWin = (int)betandwin.ExecuteScalar();

                                if (doubleExistBetAndWin > 0)
                                {
                                    betAndWinId = doubleExistBetAndWin;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", betAndWinId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, betAndWinId, betAndWinIndex, homeWin, draw, awayWin, con, values, input, closingHomeBetAndWin, closingDrawBetAndWin, closingAwayBetAndWin);
                                        }
                                    }
                                }
                            }


                            var gameBookersSelect = $"select id from bettingcompanies where BettingCompanyName = ('GameBookers')";
                            using (var gameBookers = new SqlCommand(gameBookersSelect, con))
                            {

                                if (gameBookersIndex == -1 || values[gameBookersIndex] == "")
                                {
                                    nullValue = null;
                                }
                                else
                                {
                                    homeWin = decimal.Parse(values[gameBookersIndex], culture);
                                    draw = decimal.Parse(values[gameBookersIndex + 1], culture);
                                    awayWin = decimal.Parse(values[gameBookersIndex + 2], culture);
                                }

                                int doubleExistGameBookers = (int)gameBookers.ExecuteScalar();

                                if (doubleExistGameBookers > 0)
                                {
                                    gameBookersId = doubleExistGameBookers;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", gameBookersId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, gameBookersId, gameBookersIndex, homeWin, draw, awayWin, con, values, input, closingHomeGameBookers, closingDrawGameBookers, closingAwayGameBookers);
                                        }
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
                                int doubleExistInterwetten = (int)interwetten.ExecuteScalar();

                                if (doubleExistInterwetten > 0)
                                {
                                    interwettenId = doubleExistInterwetten;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";

                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", interwettenId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {

                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, interwettenId, interwettenIndex, homeWin, draw, awayWin, con, values, input, closingHomeInterWetten, closingDrawInterWetten, closingAwayInterWetten);
                                        }
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
                                int doubleExistLadbrokers = (int)ladbrokers.ExecuteScalar();
                                if (doubleExistLadbrokers > 0)
                                {
                                    ladbrokersId = doubleExistLadbrokers;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", ladbrokersId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, ladbrokersId, ladbrokersIndex, homeWin, draw, awayWin, con, values, input, closingHomeLadbrookers, closingDrawLadbrookers, closingAwayLadbrookers);
                                        }
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
                                int doubleExistPinnacle = (int)pinnacle.ExecuteScalar();

                                if (doubleExistPinnacle > 0)
                                {
                                    pinnacleId = doubleExistPinnacle;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", pinnacleId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, pinnacleId, pinnacleIndex, homeWin, draw, awayWin, con, values, input, closingHomePinnacle, closingDrawPinnacle, closingAwayPinnacle);
                                        }
                                    }
                                }
                            }

                            var sportingOddsSelect = $"select id from bettingcompanies where BettingCompanyName = ('Sporting Odds')";
                            using (var sportingOdds = new SqlCommand(sportingOddsSelect, con))
                            {

                                if (sportingOddsIndex == -1 || values[sportingOddsIndex] == "")
                                {
                                    nullValue = null;
                                }
                                else
                                {
                                    homeWin = decimal.Parse(values[sportingOddsIndex], culture);
                                    draw = decimal.Parse(values[sportingOddsIndex + 1], culture);
                                    awayWin = decimal.Parse(values[sportingOddsIndex + 2], culture);
                                }
                                int doubleExistSportingOdds = (int)sportingOdds.ExecuteScalar();

                                if (doubleExistSportingOdds > 0)
                                {
                                    sportingOddsId = doubleExistSportingOdds;


                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", sportingOddsId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {

                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, sportingOddsId, sportingOddsIndex, homeWin, draw, awayWin, con, values, input, closingHomeSportingOdds, closingDrawSportingOdds, closingAwaySportingOdds);
                                        }
                                    }
                                }
                            }


                            var sportingBetSelect = $"select id from bettingcompanies where BettingCompanyName = ('Sportingbet')";
                            using (var sportingBet = new SqlCommand(sportingBetSelect, con))
                            {

                                if (sportingBetIndex == -1 || values[sportingBetIndex] == "")
                                {
                                    nullValue = null;
                                }
                                else
                                {
                                    homeWin = decimal.Parse(values[sportingBetIndex], culture);
                                    draw = decimal.Parse(values[sportingBetIndex + 1], culture);
                                    awayWin = decimal.Parse(values[sportingBetIndex + 2], culture);
                                }
                                int doubleExistSportingBet = (int)sportingBet.ExecuteScalar();
                                if (doubleExistSportingBet > 0)
                                {
                                    sportingBetId = doubleExistSportingBet;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", sportingBetId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, sportingBetId, sportingBetIndex, homeWin, draw, awayWin, con, values, input, closingHomeSportingBet, closingDrawSportingBet, closingAwaySportingBet);
                                        }
                                    }
                                }
                            }


                            var stanJamesSelect = $"select id from bettingcompanies where BettingCompanyName = ('Stan James')";
                            using (var stanJames = new SqlCommand(stanJamesSelect, con))
                            {

                                if (stanJamesIndex == -1 || values[stanJamesIndex] == "")
                                {
                                    nullValue = null;
                                }
                                else
                                {
                                    homeWin = decimal.Parse(values[stanJamesIndex], culture);
                                    draw = decimal.Parse(values[stanJamesIndex + 1], culture);
                                    awayWin = decimal.Parse(values[stanJamesIndex + 2], culture);
                                }
                                int doubleExistStanJames = (int)stanJames.ExecuteScalar();
                                if (doubleExistStanJames > 0)
                                {
                                    stanJamesId = doubleExistStanJames;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", stanJamesId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, stanJamesId, stanJamesIndex, homeWin, draw, awayWin, con, values, input, closingHomeStanJames, closingDrawStanJames, closingAwayStanJames);
                                        }
                                    }
                                }
                            }


                            var stanleyBetSelect = $"select id from bettingcompanies where BettingCompanyName = ('Stanleybet')";
                            using (var stanley = new SqlCommand(stanleyBetSelect, con))
                            {

                                if (stanleyBetIndex == -1 || values[stanleyBetIndex] == "")
                                {
                                    nullValue = null;
                                }
                                else
                                {
                                    homeWin = decimal.Parse(values[stanleyBetIndex], culture);
                                    draw = decimal.Parse(values[stanleyBetIndex + 1], culture);
                                    awayWin = decimal.Parse(values[stanleyBetIndex + 2], culture);
                                }
                                int doubleExistStanley = (int)stanley.ExecuteScalar();
                                if (doubleExistStanley > 0)
                                {
                                    stanleyBetId = doubleExistStanley;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", stanleyBetId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, stanleyBetId, stanleyBetIndex, homeWin, draw, awayWin, con, values, input, closingHomeStanleybey, closingDrawStanleybey, closingAwayStanleybey);
                                        }
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
                                int doubleExistWilliamHill = (int)williamHill.ExecuteScalar();

                                if (doubleExistWilliamHill > 0)
                                {
                                    williamHillId = doubleExistWilliamHill;
                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", williamHillId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, williamHillId, williamHillIndex, homeWin, draw, awayWin, con, values, input, closingHomeWilliamHill, closingDrawWilliamHill, closingAwayWilliamHill);
                                        }
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
                                int doubleExistVCBet = (int)VCBet.ExecuteScalar();

                                if (doubleExistVCBet > 0)
                                {
                                    VCBetId = doubleExistVCBet;

                                    var checkMatchIdAndBettingId = $"select count(*) from matchodds where matchId = @currentMatchId and bettingCompanyId = @bettingId ";
                                    using (var finalCheck = new SqlCommand(checkMatchIdAndBettingId, con))
                                    {
                                        finalCheck.Parameters.AddWithValue("@currentMatchId", matchId);
                                        finalCheck.Parameters.AddWithValue("@bettingId", VCBetId);

                                        int checkMatchId = (int)finalCheck.ExecuteScalar();
                                        if (checkMatchId > 0)
                                        {
                                            Console.WriteLine("Duplicates found");
                                        }
                                        else if (checkMatchId == 0)
                                        {
                                            var input = $"insert into matchodds(matchid,bettingcompanyid,hometeamwinodds,drawteamwinodds,awayteamwinodds,closinghome,closingdraw,closingaway) values (@matchId,@betId,@homeWin,@draw,@awayWin,@closingHome,@closingDraw,@closingAway)";
                                            InsertIntoMatchOdds(matchId, VCBetId, VCIndex, homeWin, draw, awayWin, con, values, input, closingHomeVCBet, closingDrawVCBet, closingAwayVCBet);
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
        }

        private static void InsertIntoMatchOdds(int matchId, int betId, int betIndex, decimal homeWin, decimal draw, decimal awayWin, SqlConnection con, string[] values, string input, int closingHomeIndex, int closingDrawIndex, int closingAwayIndex)
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
                    if (closingHomeIndex == -1)
                        inputResult.Parameters.AddWithValue("@closingHome", DBNull.Value);
                    else
                        inputResult.Parameters.AddWithValue("@closingHome", values[closingHomeIndex]);

                    if (closingDrawIndex == -1)
                        inputResult.Parameters.AddWithValue("@closingDraw", DBNull.Value);
                    else
                        inputResult.Parameters.AddWithValue("@closingDraw", values[closingDrawIndex]);

                    if (closingAwayIndex == -1)
                        inputResult.Parameters.AddWithValue("@closingAway", DBNull.Value);
                    else
                        inputResult.Parameters.AddWithValue("@closingAway", values[closingAwayIndex]);

                    inputResult.ExecuteNonQuery();
                    Console.WriteLine("Null value Pushed to database!");
                }
                else if (betIndex != -1)
                {
                    inputResult.Parameters.AddWithValue("@matchId", matchId);
                    inputResult.Parameters.AddWithValue("@betId", betId);
                    inputResult.Parameters.AddWithValue("@homeWin", Convert.ToDecimal(homeWin));
                    inputResult.Parameters.AddWithValue("@draw", Convert.ToDecimal(draw));
                    inputResult.Parameters.AddWithValue("@awayWin", Convert.ToDecimal(awayWin));

                    if (closingHomeIndex == -1)
                        inputResult.Parameters.AddWithValue("@closingHome", DBNull.Value);
                    else
                        inputResult.Parameters.AddWithValue("@closingHome", values[closingHomeIndex]);

                    if (closingDrawIndex == -1)
                        inputResult.Parameters.AddWithValue("@closingDraw", DBNull.Value);
                    else
                        inputResult.Parameters.AddWithValue("@closingDraw", values[closingDrawIndex]);

                    if (closingAwayIndex == -1)
                        inputResult.Parameters.AddWithValue("@closingAway", DBNull.Value);
                    else
                        inputResult.Parameters.AddWithValue("@closingAway", values[closingAwayIndex]);


                    inputResult.ExecuteNonQuery();
                    Console.WriteLine("Record Pushed to database!");
                }
            }
        }

        private void PushSpecificMatchData()
        {

            //TODO: kolla upp vilket index alla kolumner har:
            DateOnly MatchDate;
            string HomeTeam = "";
            string AwayTeam = "";
            int matchId = 0;
            int homeTeamId = 0;
            int awayTeamId = 0;
            int startIndex = 0;

            decimal Bb1X2 = 0;
            decimal BbMxH = 0;
            decimal BbAvH = 0;
            decimal BbMxD = 0;
            decimal BbAvD = 0;
            decimal BbMxA = 0;
            decimal BbAvA = 0;
            decimal BbOU = 0;
            decimal BbMxGT25 = 0;
            decimal BbAvGT25 = 0;
            decimal BbMxLT25 = 0;
            decimal BbAvLT25 = 0;
            decimal BbAH = 0;
            decimal BbAHH = 0;
            decimal BbMxAHH = 0;
            decimal BbAvAHH = 0;
            decimal BbMxAHA = 0;
            decimal BbAvAHA = 0;
            decimal PSCH = 0;
            decimal PSCD = 0;
            decimal PSCA = 0;
            decimal MaxH = 0;
            decimal MaxD = 0;
            decimal MaxA = 0;
            decimal AvgH = 0;
            decimal AvgD = 0;
            decimal AvgA = 0;
            decimal B365GT2Point5 = 0;
            decimal B365LT2Point5 = 0;
            decimal PGT2Point5 = 0;
            decimal PLT2Point5 = 0;
            decimal MaxGT2Point5 = 0;
            decimal MaxLT2Point5 = 0;
            decimal AvgGT2Point5 = 0;
            decimal AvgLT2Point5 = 0;
            decimal AHh = 0;
            decimal B365AHH = 0;
            decimal B365AHA = 0;
            decimal PAHH = 0;
            decimal PAHA = 0;
            decimal MaxAHH = 0;
            decimal MaxAHA = 0;
            decimal AvgAHH = 0;
            decimal AvhAHA = 0;
            decimal MaxCH = 0;
            decimal MaxCD = 0;
            decimal MaxCA = 0;
            decimal AvgCH = 0;
            decimal AvgCD = 0;
            decimal AvgCA = 0;
            decimal Bet365CGT2Point5 = 0;
            decimal Bet365CLT2Point5 = 0;
            decimal PCGT2Point5 = 0;
            decimal PCLT2Point5 = 0;
            decimal MaxCGT2Point5 = 0;
            decimal MaxCLT2Point5 = 0;
            decimal AvgCGT2Point5 = 0;
            decimal AvgCLT2Point5 = 0;
            decimal AHCH = 0;
            decimal B365CAHH = 0;
            decimal B365CAHA = 0;
            decimal PCAHH = 0;
            decimal PCAHA = 0;
            decimal MaxCAHH = 0;
            decimal MaxCAHA = 0;
            decimal AvgCAHH = 0;
            decimal AvgCAHA = 0;




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
                            var doubleRecord = $"select count(*) from SpecificBettingOdds where matchId = ('{matchId}')";
                            using (var first = new SqlCommand(doubleRecord, con))
                            {
                                int doubleValues = (int)first.ExecuteScalar();
                                if (doubleValues > 0)
                                {
                                    Console.WriteLine("Duplicates found!");
                                }
                                else if (doubleValues == 0)
                                {
                                     Bb1X2 = decimal.Parse(specificList[0], culture);
                                     BbMxH = decimal.Parse(specificList[1], culture);
                                     BbAvH = decimal.Parse(specificList[2], culture);
                                     BbMxD = decimal.Parse(specificList[3], culture);
                                     BbAvD = decimal.Parse(specificList[4], culture);
                                     BbMxA = decimal.Parse(specificList[5], culture);
                                     BbAvA = decimal.Parse(specificList[6], culture);
                                     BbOU = decimal.Parse(specificList[7], culture);
                                     BbMxGT25 = decimal.Parse(specificList[8], culture);
                                     BbAvGT25 = decimal.Parse(specificList[9], culture);
                                     BbMxLT25 = decimal.Parse(specificList[10], culture);
                                     BbAvLT25 = decimal.Parse(specificList[11], culture);
                                     BbAH = decimal.Parse(specificList[12], culture);
                                     BbAHH = decimal.Parse(specificList[13], culture);
                                     BbMxAHH = decimal.Parse(specificList[14], culture);
                                     BbAvAHH = decimal.Parse(specificList[15], culture);
                                     BbMxAHA = decimal.Parse(specificList[16], culture);
                                     BbAvAHA = decimal.Parse(specificList[17], culture);
                                     PSCH = decimal.Parse(specificList[18], culture);
                                     PSCD = decimal.Parse(specificList[19], culture);
                                     PSCA = decimal.Parse(specificList[20], culture);

                                    var input = $"insert into specificbettingodds(matchId,bB1X2,BbMxH,BbAvH,BbMxD,BbAvD,BbMxA,BbAvA,BbOU,BbMxGT25,BbAvGT25,BbMxLT25,BbAvLT25,BbAH,BbAHH,BbMxAHH,BbAvAHH,BbMxAHA,BbAvAHA,PSCH,PSCD,PSCA)" +
                                        $"values (@matchId,@bB1x2,@BbMxH,@BbAvH,@BbMxD,@BbAvD,@BbMxA,@BbAvA,@BbOU,@BbMxGT25,@BbAvGT25,@BbMxLT25,@BbAvLT25,@BbAH,@BbAHH,@BbMxAHH,@BbAvAHH,@BbMxAHA,@BbAvAHA,@PSCH,@PSCD,@PSCA)";

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


        private void PushDataToMatches()
        {
            int attendenceIndex = 0;

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

                    if (counter > 0)
                    {
                        if (values[1].Length <= 8)
                            MatchDate = ConvertDate(values);
                        else
                            MatchDate = (DateOnly.ParseExact(values[1], "dd/MM/yyyy", CultureInfo.InvariantCulture));

                        HomeTeam = (values[2]);
                        AwayTeam = (values[3]);
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
                                        Console.WriteLine("Record already exist");
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
                                                    Console.WriteLine("Record already exists!");
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
                                                        Console.WriteLine("Record Pushed to database!");
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
                                                Console.WriteLine("Record Pushed to database!");
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
                                            Console.WriteLine("Record already exists!");
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
                                                        Console.WriteLine("Record already exists!");
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
                                                            Console.WriteLine("Record Pushed to database!");
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
                                                    Console.WriteLine("Record Pushed to database!");
                                                }
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
