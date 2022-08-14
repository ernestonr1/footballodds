using FootballManager;
using SqlOperations;
using System.Data.SqlClient;





string connectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=FootballManager;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";

DisplayUI display = new DisplayUI(connectionString);
display.Run();




