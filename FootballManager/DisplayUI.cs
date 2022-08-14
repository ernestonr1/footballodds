using Spectre.Console;
using SqlOperations;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FootballManager
{
    public class DisplayUI
    {
        public string DatabaseConnectionString { get; set; }

        public DisplayUI(string databaseConnectionString)
        {
            DatabaseConnectionString = databaseConnectionString;
        }


        public void Run()
        {
            RunAgain();
        }


        private void DisplayAllTeamsFromSeasons()
        {

            var table = new Table();
            table.Border(TableBorder.HeavyEdge);
            table.AddColumn("Team Name");

            var season = "";
            int seasonId = 0;

            List<string> seasons = new List<string>();
            using (SqlConnection con = new SqlConnection(DatabaseConnectionString))
            {
                con.Open();
                var query = @"select seasonName from Seasons";
                using (SqlCommand command = new SqlCommand(query, con))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            seasons.Add(reader.GetString(0));
                        }
                    }
                }

                season = AnsiConsole.Prompt(
                       new SelectionPrompt<string>()
                       .Title("Which Season Do you want to select?")
                       .PageSize(10)
                       .AddChoices(seasons));
                AnsiConsole.WriteLine($"You selected {season}");
            }

            var selectId = $"select id from seasons where seasonName = ('{season}')";
            using (SqlConnection con = new SqlConnection(DatabaseConnectionString))
            {
                con.Open();
                using (SqlCommand command = new SqlCommand(selectId, con))
                {
                    int rowExist = (int)command.ExecuteScalar();
                    if (rowExist > 0)
                    {
                        seasonId = rowExist;
                    }
                }
            }



            using (SqlConnection con = new SqlConnection(DatabaseConnectionString))
            {
                con.Open();
                var query = @"select distinct TeamName from Teams
                            inner join Matches
                            on Matches.homeTeamId = Teams.Id or Matches.awayTeamId = Teams.Id
                            inner join Seasons on Matches.seasonId = Seasons.Id 
                            where Seasons.Id = @seasonId";
                using (SqlCommand command = new SqlCommand(query, con))
                {
                    command.Parameters.AddWithValue("@seasonId", seasonId);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            table.AddRow(reader.GetString(0));
                        }
                    }
                }
            }
            AnsiConsole.Clear();
            AnsiConsole.Write(table);
        }


        private void DisplayAllMatchesForHomeTeam()
        {
            var team = "";
            var teamId = 0;



            var table = new Table();
            table.Border(TableBorder.HeavyEdge);
            table.AddColumn("Home Team");
            table.AddColumn(new TableColumn("Away Team"));
            table.AddColumn(new TableColumn("Date"));
            table.AddColumn(new TableColumn("FTR"));

            List<string> teams = new List<string>();
            List<object> matches = new List<object>();
            using (SqlConnection con = new SqlConnection(DatabaseConnectionString))
            {
                con.Open();
                var query = @"select TeamName from teams";
                using (SqlCommand command = new SqlCommand(query, con))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            teams.Add(reader.GetString(0));
                        }
                    }
                }

                team = AnsiConsole.Prompt(
                       new SelectionPrompt<string>()
                       .Title("Which Team Do you want to select?")
                       .PageSize(10)
                       .AddChoices(teams));
                AnsiConsole.WriteLine($"You selected {team}");
            }

            var selectId = $"select id from teams where TeamName = ('{team}')";
            using (SqlConnection con = new SqlConnection(DatabaseConnectionString))
            {
                con.Open();
                using (SqlCommand command = new SqlCommand(selectId, con))
                {
                    int rowExist = (int)command.ExecuteScalar();
                    if (rowExist > 0)
                    {
                        teamId = rowExist;
                    }
                }
                int counter = 0;
                string queryy = $"select homeTeamId,awayTeamId,matchDate,FTR from Matches inner join Teams on Matches.homeTeamId = Teams.Id where Teams.Id = ('{teamId}')";
                using (SqlCommand command = new SqlCommand(queryy, con))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            table.AddRow(reader.GetInt32(0).ToString(), reader.GetInt32(1).ToString(), reader.GetDateTime(2).ToString(), reader.GetString(3));
                        }
                    }
                }
            }
            AnsiConsole.Clear();
            AnsiConsole.Write(table);
        }

        private void DisplayAllMatchesForHomeWinOdds()
        {
            string query = @"select Matches.Id,homeTeamId,awayTeamId,FTR,MatchOdds.interwettenHomeTeamWinOdds,MatchOdds.williamHillHomeTeamWinOdds
                            from Matches
                            inner join MatchOdds on Matches.Id = MatchOdds.matchId
                            where MatchOdds.interwettenHomeTeamWinOdds > 1.8 and MatchOdds.williamHillHomeTeamWinOdds > 1.8 
                            order by MatchOdds.interwettenHomeTeamWinOdds";
        }

        private void DisplayAllMatchesForHomeTeamLost()
        {

            var team = "";
            var teamId = 0;

            var table = new Table();
            table.Border(TableBorder.HeavyEdge);
            table.AddColumn("Home Team");
            table.AddColumn(new TableColumn("Away Team"));
            table.AddColumn(new TableColumn("Date"));
            table.AddColumn(new TableColumn("FTR"));

            List<string> teams = new List<string>();
            List<object> matches = new List<object>();
            using (SqlConnection con = new SqlConnection(DatabaseConnectionString))
            {
                con.Open();
                var queryy = @"select TeamName from teams";
                using (SqlCommand command = new SqlCommand(queryy, con))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            teams.Add(reader.GetString(0));
                        }
                    }
                }

                team = AnsiConsole.Prompt(
                       new SelectionPrompt<string>()
                       .Title("Which Team Do you want to select?")
                       .PageSize(10)
                       .AddChoices(teams));
                AnsiConsole.WriteLine($"You selected {team}");
            }

            var selectId = $"select id from teams where TeamName = ('{team}')";
            using (SqlConnection con = new SqlConnection(DatabaseConnectionString))
            {
                con.Open();
                using (SqlCommand command = new SqlCommand(selectId, con))
                {
                    int rowExist = (int)command.ExecuteScalar();
                    if (rowExist > 0)
                    {
                        teamId = rowExist;
                    }
                }
                string query = $"select homeTeamId,awayTeamId,matchDate,FTR " +
                    $"from Matches inner join Teams on dbo.Matches.homeTeamId = dbo.Teams.Id " +
                    $"where homeTeamId = ('{teamId}') and FTR = 'A'";
                using (SqlCommand command = new SqlCommand(query, con))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            table.AddRow(reader.GetInt32(0).ToString(), reader.GetInt32(1).ToString(), reader.GetDateTime(2).ToString(), reader.GetString(3));
                        }
                    }
                }
            }
            AnsiConsole.Clear();
            AnsiConsole.Write(table);
        }

        private string MenuChoices()
        {
            return AnsiConsole.Prompt(
                                new SelectionPrompt<string>()
                                    .Title("Menu Choices")
                                    .PageSize(10)
                                    .MoreChoicesText("Scroll down for more alternatives")
                                    .AddChoices(new[] {
            "1. Create Database",
            "2. Populate Database",
            "3. Display All Teams from a season",
            "4. Display All matches for home team",
            "5. Display All matches where home team lost",
            "Q. Quit program"
                                    }));
        }


        public void PopulateDatabase()
        {
            //string testPath = @"C:\Users\patri\Downloads\Patrik\Patrik\pl-09-10.csv";
            string testPath = @"C:\Users\patri\Downloads\Patrik\Patrik\pl-21-22.csv";

            //string testPath = @"C:\Users\patri\Downloads\Patrik\pl-14-15.csv";


            string filePath = @"C:\Users\patri\Desktop\LIA - Uppgift\Bundesliga1-2017-2018.csv";
            string connectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=FootballManager;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";

            SqlInserts inserts = new SqlInserts(filePath, connectionString, isEnglish: true);
            SqlInsertsMatches matchesInserts = new SqlInsertsMatches(filePath, connectionString, isEnglish: true);
            inserts.PushToDatabase();
            matchesInserts.PushDataForMatchesToDatabase();


            AnsiConsole.Status()
            .Start("Waiting...", ctx =>
            {
                // Simulate some work
                AnsiConsole.MarkupLine("Waiting...");
                Thread.Sleep(1000);

                // Update the status and spinner
                ctx.Status("Waiting");
                ctx.Spinner(Spinner.Known.Star);
                ctx.SpinnerStyle(Style.Parse("green"));

                // Simulate some work
                AnsiConsole.MarkupLine("Waiting...");
                Thread.Sleep(2000);
            });
            Console.Clear();
        }

        public void RunAgain()
        {

            SqlCreation create = new SqlCreation(DatabaseConnectionString);

            bool loop = true;
            while (loop)
            {
                Console.Clear();
                string menu = MenuChoices();
                switch (menu[0])
                {
                    case '1':
                        create.CreateDatabase();
                        break;
                    case '2':
                        PopulateDatabase();
                        break;
                    case '3':
                        DisplayAllTeamsFromSeasons();
                        Console.WriteLine("Press a key to continue...");
                        Console.ReadLine();
                        break;
                    case '4':
                        DisplayAllMatchesForHomeTeam();
                        Console.WriteLine("Press a key to continue...");
                        Console.ReadLine();
                        break;
                    case '5':
                        DisplayAllMatchesForHomeTeamLost();
                        Console.WriteLine("Press a key to continue...");
                        Console.ReadLine();
                        break;
                    case 'Q':
                        loop = false;
                        break;

                    default:
                        break;
                }


            }
        }
    }
}
