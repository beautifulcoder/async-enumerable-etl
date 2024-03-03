using System.Data;
using Dapper;
using System.Data.SQLite;
using System.IO.Compression;

SqlMapper.AddTypeHandler(new GuidTypeHandler());
SqlMapper.RemoveTypeMap(typeof(Guid));
SqlMapper.RemoveTypeMap(typeof(Guid?));

var mode = args.Length > 0 ? args[0] : "IEnumerable";

switch (mode)
{
  case "IEnumerable":
    await WriteToCsvFileWithIEnumerable();
    break;

  case "IAsyncEnumerable":
    await WriteToCsvFileWithIAsyncEnumerable();
    break;

  default:
    Console.WriteLine("Invalid mode");
    break;
}

return;

static async Task WriteToCsvFileWithIEnumerable()
{
  await using var fileStream = File.Create("etl-salesorderdetail.csv.gz");
  await using var compressionStream = new GZipStream(fileStream, CompressionLevel.Optimal);
  await using var writer = new StreamWriter(compressionStream);

  foreach (var detail in await GetSalesOrderDetailsWithIEnumerable())
  {
    await writer.WriteAsync(GetSalesOrderDetailRow(detail));
  }
}

static async Task WriteToCsvFileWithIAsyncEnumerable()
{
  await using var fileStream = File.Create("etl-salesorderdetail.csv.gz");
  await using var compressionStream = new GZipStream(fileStream, CompressionLevel.Optimal);
  await using var writer = new StreamWriter(compressionStream);

  await using var extract = GetSalesOrderDetailsWithIAsyncEnumerable();

  await foreach (var detail in extract.Data)
  {
    await writer.WriteAsync(GetSalesOrderDetailRow(detail));
  }
}

static string GetSalesOrderDetailRow(SalesOrderDetail detail) =>
  $"{detail.SalesOrderId},{detail.SalesOrderDetailId},{detail.CarrierTrackingNumber},{detail.OrderQty},{detail.ProductId},{detail.SpecialOfferId},{detail.UnitPrice:F2},{detail.UnitPriceDiscount:F2},{detail.LineTotal:F2},{detail.RowGuid},{detail.ModifiedDate.ToUniversalTime():o}\r\n";

static async Task<IEnumerable<SalesOrderDetail>> GetSalesOrderDetailsWithIEnumerable()
{
  const string sql = """
    SELECT
      SalesOrderId,
      SalesOrderDetailId,
      CarrierTrackingNumber,
      OrderQty,
      ProductId,
      SpecialOfferId,
      UnitPrice,
      UnitPriceDiscount,
      LineTotal,
      RowGuid,
      ModifiedDate
    FROM SalesOrderDetail
  """;

  await using var connection = new SQLiteConnection("Data Source=AdventureWorks.db");

  return await connection.QueryAsync<SalesOrderDetail>(sql);
}

static StreamedQuery<SalesOrderDetail> GetSalesOrderDetailsWithIAsyncEnumerable()
{
  const string sql = """
    SELECT
      SalesOrderId,
      SalesOrderDetailId,
      CarrierTrackingNumber,
      OrderQty,
      ProductId,
      SpecialOfferId,
      UnitPrice,
      UnitPriceDiscount,
      LineTotal,
      RowGuid,
      ModifiedDate
    FROM SalesOrderDetail
  """;

  var connection = new SQLiteConnection("Data Source=AdventureWorks.db");

  return new StreamedQuery<SalesOrderDetail>(
    connection,
    connection.QueryUnbufferedAsync<SalesOrderDetail>(sql));
}

public class StreamedQuery<T>(
  IAsyncDisposable connection,
  IAsyncEnumerable<T> data) : IAsyncDisposable
{
  public IAsyncEnumerable<T> Data { get; } = data;

  public ValueTask DisposeAsync() => connection.DisposeAsync();
}

public class GuidTypeHandler : SqlMapper.TypeHandler<Guid>
{
  public override void SetValue(IDbDataParameter parameter, Guid guid)
  {
    parameter.Value = guid.ToString();
  }

  public override Guid Parse(object value)
  {
    return new Guid((string)value);
  }
}

public class SalesOrderDetail
{
  public required int SalesOrderId { get; init; }
  public required int SalesOrderDetailId { get; init; }
  public string? CarrierTrackingNumber { get; init; }
  public required short OrderQty { get; init; }
  public required int ProductId { get; init; }
  public required int SpecialOfferId { get; init; }
  public required decimal UnitPrice { get; init; }
  public required decimal UnitPriceDiscount { get; init; }
  public required decimal LineTotal { get; init; }
  public required Guid RowGuid { get; init; }
  public required DateTime ModifiedDate { get; init; }
}
