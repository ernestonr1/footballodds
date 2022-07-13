using System.Data.SqlClient;

List<string> firstRow = new List<string>();


using (SqlConnection con = new SqlConnection(@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=FootballManager;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
{
    con.Open();


    using (StreamReader reader = new StreamReader(@"C:\Users\patri\Desktop\LIA - Uppgift\Premier_league-2017-2018.csv"))
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
        var result = firstRow.Distinct().ToList();

        foreach (var item in result)
        {
            var query = $"insert into Teams(TeamName) values('{item}')";
            using (var command = new SqlCommand(query, con))
            {
                command.ExecuteNonQuery();
            }
        }


 
    }
    con.Close();
}



//foreach (var iem in result)
//{
//    Console.WriteLine(item);
//}