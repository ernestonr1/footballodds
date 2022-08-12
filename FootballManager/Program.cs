using SqlOperations;
using System.Data.SqlClient;




//string testPath = @"C:\Users\patri\Downloads\Patrik\Patrik\pl-09-10.csv";
string testPath = @"C:\Users\patri\Downloads\Patrik\Patrik\pl-21-22.csv";

//string testPath = @"C:\Users\patri\Downloads\Patrik\pl-14-15.csv";


string filePath = @"C:\Users\patri\Desktop\LIA - Uppgift\Bundesliga1-2017-2018.csv";
string connectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=FootballManager;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";

SqlInserts inserts = new SqlInserts(filePath, connectionString, isEnglish: true);
SqlInsertsMatches matchesInserts = new SqlInsertsMatches(filePath, connectionString, isEnglish: true);


inserts.PushToDatabase();
matchesInserts.PushDataForMatchesToDatabase();
