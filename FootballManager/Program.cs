using SqlOperations;
using System.Data.SqlClient;


string testPath = @"C:\Users\patri\Downloads\Patrik\Patrik\pl-20-21.csv";
string filePath = @"C:\Users\patri\Desktop\LIA - Uppgift\Premier_league-2017-2018.csv";
string connectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=FootballManager;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";

SqlInserts inserts = new SqlInserts(testPath, connectionString, isEnglish: true);
SqlInsertsMatches matchesInserts = new SqlInsertsMatches(testPath, connectionString, isEnglish: true);


inserts.PushToDatabase();
matchesInserts.PushDataForMatchesToDatabase();
