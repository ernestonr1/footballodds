using System.Data.SqlClient;

List<string> firstRow = new List<string>();


using (SqlConnection con = new SqlConnection(@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=FootballManager;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
{
    con.Open();
    using(StreamReader reader = new StreamReader(@"C:\Users\patri\Desktop\LIA-uppgift\Premier_league-2017-2018.csv"))
    {
        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            var values = line.Split(';',',');
            firstRow.Add(values[2].ToString());
            //var query = "insert into blblb";
            //using(var command = new SqlCommand(query, con))
            //{
            //    command.ExecuteNonQuery();
            //}
        }
    }
    con.Close();
}

var result = firstRow.Distinct().ToList();


foreach (var item in result)
{
    Console.WriteLine(item);
}