using SqlOperations;
using System.Data.SqlClient;

SqlInserts inserts = new SqlInserts(@"C:\Users\patri\Desktop\LIA-uppgift\Premier_league-2017-2018.csv",
                                    @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=FootballManager;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;
                                    ApplicationIntent=ReadWrite;MultiSubnetFailover=False",isEnglish: true);


//inserts.PushToCountries();
//inserts.PushTeamsToDatabase();
//inserts.PushToLeagues();
//inserts.PushDataToSeasons();
inserts.PushDataToMatches();

