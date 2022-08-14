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
            }
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

        private void CreateCountries()
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
        private void CreateTeams()
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

        private void CreateLeagues()
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

        private void CreateSeasons()
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


        private void CreateMatches()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"CREATE TABLE [dbo].[Matches](
	                            [Id] [int] IDENTITY(1,1) NOT NULL,
	                            [homeTeamId] [int] NOT NULL,
	                            [awayTeamId] [int] NOT NULL,
	                            [seasonId] [int] NOT NULL,
	                            [matchDate] [date] NOT NULL,
	                            [FTHG] [int] NOT NULL,
	                            [FTAG] [int] NOT NULL,
	                            [FTR] [nvarchar](50) NOT NULL,
	                            [HTHG] [int] NOT NULL,
	                            [HTAG] [int] NOT NULL,
	                            [HTR] [nvarchar](50) NOT NULL,
	                            [REFEREE] [nvarchar](50) NULL,
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
                                ) ON [PRIMARY]]";
                using (SqlCommand cmd = new SqlCommand(query, con))
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void CreateBettingCompanies()
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

        private void CreateMatchOdds()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"CREATE TABLE [dbo].[MatchOdds](
	                            [Id] [int] IDENTITY(1,1) NOT NULL,
	                            [matchId] [int] NOT NULL,
	                            [bettingCompanyId] [int] NOT NULL,
	                            [homeTeamWinOdds] [decimal](18, 4) NULL,
	                            [drawTeamWinOdds] [decimal](18, 4) NULL,
	                            [awayTeamWinOdds] [decimal](18, 4) NULL,
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

        private void CreateSpecificMatchData()
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                string query = @"CREATE TABLE [dbo].[SpecificBettingOdds](
	                           [Id] [int] IDENTITY(1,1) NOT NULL,
	                           [matchId] [int] NULL,
	                           [Bb1X2] [float] NULL,
	                           [BbMxH] [float] NULL,
	                           [BbAvH] [float] NULL,
	                           [BbMxD] [float] NULL,
	                           [BbAvD] [float] NULL,
	                           [BbMxA] [float] NULL,
	                           [BbAvA] [float] NULL,
	                           [BbOU] [float] NULL,
	                           [BbMxGT25] [float] NULL,
	                           [BbAvGT25] [float] NULL,
	                           [BbMxLT25] [float] NULL,
	                           [BbAvLT25] [float] NULL,
	                           [BbAH] [float] NULL,
	                           [BbAHH] [float] NULL,
	                           [BbMxAHH] [float] NULL,
	                           [BbAvAHH] [float] NULL,
	                           [BbMxAHA] [float] NULL,
	                           [BbAvAHA] [float] NULL,
	                           [PSCH] [float] NULL,
	                           [PSCD] [float] NULL,
	                           [PSCA] [float] NULL,
                               CONSTRAINT [PK_SpecificBettingOdds] PRIMARY KEY CLUSTERED 
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
    }
}






