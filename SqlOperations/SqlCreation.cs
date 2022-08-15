using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlOperations
{
    public class SqlCreation
    {

        public string ConnectionString { get; set; }
        public SqlCreation(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public void CreateDatabase()
        {
            string connectionStringDb = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=master;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";


            var isCreated = CheckDatabaseExists("FootballManager");
            if (isCreated)
            {
                Console.WriteLine("Already Created");
            }
            else
            {

                using (SqlConnection con = new SqlConnection(connectionStringDb))
                {
                    string query = @"CREATE DATABASE FootballManager";
                    using (SqlCommand cmd = new SqlCommand(query, con))
                    {
                        con.Open();
                        cmd.ExecuteNonQuery();
                    }
                }

                CreateAllTablesAndRelationships();
            }
        }

        private void CreateAllTablesAndRelationships()
        {
            CreateCountries();
            CreateTeams();
            CreateLeagues();
            CreateSeasons();
            CreateMatches();
            CreateBettingCompanies();
            CreateMatchOdds();
            CreateRelations();
        }


        private bool CheckDatabaseExists(string dataBase)
        {
            string conStr = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=master;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";
            string cmdText = "SELECT * FROM master.dbo.sysdatabases WHERE name ='" + dataBase + "'";
            bool isExist = false;
            using (SqlConnection con = new SqlConnection(conStr))
            {
                con.Open();
                using (SqlCommand cmd = new SqlCommand(cmdText, con))
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        isExist = reader.HasRows;
                    }
                }
                con.Close();
            }
            return isExist;
        }

        public void CreateCountries()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = $"CREATE TABLE [dbo].[Countries](" +
                    $"[Id][int] IDENTITY(1, 1) NOT NULL," +
                    $"[name] [nvarchar] (50) NOT NULL," +
                    $"CONSTRAINT[PK_Countries] PRIMARY KEY CLUSTERED" +
                    $"(" +
                    $"[Id] ASC" +
                    $")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON[PRIMARY]) ON [PRIMARY]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        //teams,leagues,seasons
        public void CreateTeams()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = $"CREATE TABLE [dbo].[Teams](" +
                    $"[Id] [int] IDENTITY(1, 1) NOT NULL," +
                    $"[TeamName] [nvarchar] (50) NOT NULL," +
                    $"[CountryId] [int] NOT NULL," +
                    $"CONSTRAINT[PK_Teams] PRIMARY KEY CLUSTERED" +
                    $"(" +
                    $"[Id] ASC" +
                    $")WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON[PRIMARY]" +
                    $") ON[PRIMARY]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        
        public void CreateLeagues()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"CREATE TABLE [dbo].[Leagues](
	                            [Id] [int] IDENTITY(1,1) NOT NULL,
	                            [div] [nvarchar](10) NOT NULL,
	                            [countryId] [int] NOT NULL,
                                CONSTRAINT [PK_Leagues] PRIMARY KEY CLUSTERED 
                                (
	                            [Id] ASC
                                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                                ) ON [PRIMARY]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        
        public void CreateSeasons()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"CREATE TABLE [dbo].[Seasons](
	                            [Id] [int] IDENTITY(1,1) NOT NULL,
	                            [startDate] [date] NOT NULL,
	                            [endDate] [date] NOT NULL,
	                            [seasonName] [nvarchar](50) NOT NULL,
	                            [leagueId] [int] NOT NULL,
                                CONSTRAINT [PK_Seasons] PRIMARY KEY CLUSTERED 
                                (
	                            [Id] ASC
                                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                                ) ON [PRIMARY]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        
        
        public void CreateMatches()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"CREATE TABLE [dbo].[Matches](
	                            [Id] [int] IDENTITY(1,1) NOT NULL,
	                            [homeTeamId] [int] NOT NULL,
	                            [awayTeamId] [int] NOT NULL,
	                            [seasonId] [int] NOT NULL,
	                            [matchDate] [date] NOT NULL,
                                [matchTime] [time](7) NULL,
	                            [FTHG] [int] NOT NULL,
	                            [FTAG] [int] NOT NULL,
	                            [FTR] [nvarchar](50) NOT NULL,
	                            [HTHG] [int] NOT NULL,
	                            [HTAG] [int] NOT NULL,
	                            [HTR] [nvarchar](50) NOT NULL,
	                            [REFEREE] [nvarchar](50) NULL,
                                [Attendence] [int] NULL,
	                            [HS] [int] NOT NULL,
	                            [AS] [int] NOT NULL,
	                            [HST] [int] NOT NULL,
	                            [AST] [int] NOT NULL,
	                            [HF] [int] NOT NULL,
	                            [AF] [int] NOT NULL,
	                            [HC] [int] NOT NULL,
	                            [AC] [int] NOT NULL,
	                            [HY] [int] NOT NULL,
	                            [AY] [int] NOT NULL,
	                            [HR] [int] NOT NULL,
	                            [AR] [int] NOT NULL,
                                CONSTRAINT [PK_Matches] PRIMARY KEY CLUSTERED 
                                (
	                            [Id] ASC
                                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                                ) ON [PRIMARY]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        
        public void CreateBettingCompanies()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"CREATE TABLE [dbo].[BettingCompanies](
	                            [Id] [int] IDENTITY(1,1) NOT NULL,
	                            [BettingCompanyName] [nvarchar](50) NOT NULL,
                                CONSTRAINT [PK_BettingCompanies] PRIMARY KEY CLUSTERED 
                                (
	                            [Id] ASC
                                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                                ) ON [PRIMARY]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        
        public void CreateMatchOdds()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"CREATE TABLE [dbo].[MatchOdds](
	                            [Id] [int] IDENTITY(1,1) NOT NULL,
	                            [matchId] [int] NOT NULL,
	                            [bettingCompanyId] [int] NULL,
                                [interwettenHomeTeamWinOdds] [decimal](18, 4) NULL,
	                            [interwettenDrawTeamWinOdds] [decimal](18, 4) NULL,
	                            [interwettenAwayTeamWinOdds] [decimal](18, 4) NULL,
	                            [williamHillHomeTeamWinOdds] [decimal](18, 4) NULL,
	                            [williamHillDrawTeamWinOdds] [decimal](18, 4) NULL,
	                            [williamHillAwayTeamWinOdds] [decimal](18, 4) NULL,
                                CONSTRAINT [PK_MatchOdds] PRIMARY KEY CLUSTERED 
                                (
	                            [Id] ASC
                                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                                ) ON [PRIMARY]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public void CreateRelations()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"ALTER TABLE [dbo].[Leagues]  WITH CHECK ADD  CONSTRAINT [FK_Leagues_Countries] FOREIGN KEY([countryId])
                                REFERENCES [dbo].[Countries] ([Id])";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using(SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"ALTER TABLE [dbo].[Leagues] CHECK CONSTRAINT [FK_Leagues_Countries]";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {

                string query = @" ALTER TABLE [dbo].[Matches]  WITH CHECK ADD  CONSTRAINT [FK_Matches_Seasons] FOREIGN KEY([seasonId])
                                REFERENCES [dbo].[Seasons] ([Id])";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"ALTER TABLE [dbo].[Matches] CHECK CONSTRAINT [FK_Matches_Seasons]";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {

                string query = @" ALTER TABLE [dbo].[Matches]  WITH CHECK ADD  CONSTRAINT [FK_Matches_Teams] FOREIGN KEY([homeTeamId])
                                REFERENCES [dbo].[Teams] ([Id])";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {

                string query = @"ALTER TABLE [dbo].[Matches] CHECK CONSTRAINT [FK_Matches_Teams]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"ALTER TABLE [dbo].[Matches]  WITH CHECK ADD  CONSTRAINT [FK_Matches_Teams1] FOREIGN KEY([awayTeamId])
                                REFERENCES [dbo].[Teams] ([Id])";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"ALTER TABLE [dbo].[Matches] CHECK CONSTRAINT [FK_Matches_Teams1]";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"ALTER TABLE [dbo].[MatchOdds]  WITH CHECK ADD  CONSTRAINT [FK_MatchOdds_BettingCompanies] FOREIGN KEY([bettingCompanyId])
                                REFERENCES [dbo].[BettingCompanies] ([Id])";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                                
                string query = @"ALTER TABLE [dbo].[MatchOdds] CHECK CONSTRAINT [FK_MatchOdds_BettingCompanies]";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"ALTER TABLE [dbo].[MatchOdds]  WITH CHECK ADD  CONSTRAINT [FK_MatchOdds_Matches] FOREIGN KEY([matchId])
                                REFERENCES [dbo].[Matches] ([Id])";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"ALTER TABLE [dbo].[MatchOdds] CHECK CONSTRAINT [FK_MatchOdds_Matches]";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"ALTER TABLE [dbo].[Seasons]  WITH CHECK ADD  CONSTRAINT [FK_Seasons_Leagues1] FOREIGN KEY([leagueId])
                                REFERENCES [dbo].[Leagues] ([Id])";

                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {

                string query = @"ALTER TABLE [dbo].[Seasons] CHECK CONSTRAINT [FK_Seasons_Leagues1]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {

                string query = @"ALTER TABLE [dbo].[Teams]  WITH CHECK ADD  CONSTRAINT [FK_Teams_Countries] FOREIGN KEY([CountryId])
                                REFERENCES [dbo].[Countries] ([Id])";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }

            using (SqlConnection con = new SqlConnection(ConnectionString))
            {

                string query = @"ALTER TABLE [dbo].[Teams] CHECK CONSTRAINT [FK_Teams_Countries]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }


        }


    }
}






