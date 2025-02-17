using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using ClosedXML.Excel;
using ExcelDataReader;
using Newtonsoft.Json.Linq;

class Program
{
    private static readonly HttpClient client = new HttpClient();
    private const string ExcelFilePath = @"C:\Users\User\Desktop\grid.xlsx";
    private const int MaxRetries = 3;
    private const int BaseDelayMs = 2000;

    static async Task Main(string[] args)
    {
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

        List<Point> points = ReadExcelFile(ExcelFilePath);
        Console.WriteLine($"Загружено {points.Count} точек из Excel файла.");

        var results = new List<(Point Point, Settlement Settlement, string ErrorMessage)>();
        int processed = 0, errors = 0;

        foreach (var point in points)
        {
            var result = await FindNearestSettlementWithRetry(point);
            results.Add((point, result.Settlement, result.ErrorMessage));

            if (result.Settlement != null)
            {
                var popInfo = result.Settlement.Population.HasValue
                    ? $"Население: {result.Settlement.Population:N0}"
                    : "Население: Н/Д";
                Console.WriteLine($"[{processed + 1}/{points.Count}] {result.Settlement.Name} " +
                                $"{result.Settlement.Distance:F2} км | {popInfo}");
            }
            else
            {
                Console.WriteLine($"[{processed + 1}/{points.Count}] Ошибка: {result.ErrorMessage}");
                errors++;
            }

            processed++;
            await Task.Delay(1000);
        }

        SaveResults(results, ExcelFilePath);
        Console.WriteLine($"\nГотово! Обработано: {processed}, Ошибок: {errors}");
    }

    static List<Point> ReadExcelFile(string path)
    {
        var points = new List<Point>();

        using (var stream = File.Open(path, FileMode.Open, FileAccess.Read))
        using (var reader = ExcelReaderFactory.CreateReader(stream))
        {
            while (reader.Read())
            {
                if (reader.FieldCount < 2) continue;

                var lonStr = reader.GetValue(0)?.ToString().Replace(',', '.');
                var latStr = reader.GetValue(1)?.ToString().Replace(',', '.');

                if (double.TryParse(lonStr, NumberStyles.Any, CultureInfo.InvariantCulture, out var lon) &&
                    double.TryParse(latStr, NumberStyles.Any, CultureInfo.InvariantCulture, out var lat))
                {
                    points.Add(new Point(lon, lat));
                }
            }
        }

        return points;
    }

    static async Task<(Settlement Settlement, string ErrorMessage)> FindNearestSettlementWithRetry(Point point)
    {
        int retry = 0;
        while (retry < MaxRetries)
        {
            var result = await FindNearestSettlement(point);
            if (result.ErrorMessage == null || !result.ErrorMessage.Contains("Too Many Requests"))
                return result;

            await Task.Delay(BaseDelayMs * (retry + 1));
            retry++;
        }
        return (null, "Превышено количество попыток запроса");
    }

    static async Task<(Settlement Settlement, string ErrorMessage)> FindNearestSettlement(Point point)
    {
        try
        {
            var (lon, lat) = (NormalizeLongitude(point.Longitude), NormalizeLatitude(point.Latitude));
            if (Math.Abs(lat) > 90) return (null, "Некорректные координаты");

            var query = $@"
                [out:json][timeout:30];
                (
                    node[place~'city|town'](around:100000, {lat.ToString(CultureInfo.InvariantCulture)}, {lon.ToString(CultureInfo.InvariantCulture)});
                    way[place~'city|town'](around:100000, {lat.ToString(CultureInfo.InvariantCulture)}, {lon.ToString(CultureInfo.InvariantCulture)});
                    relation[place~'city|town'](around:100000, {lat.ToString(CultureInfo.InvariantCulture)}, {lon.ToString(CultureInfo.InvariantCulture)});
                );
                out center;
            ";

            var response = await client.PostAsync("https://overpass-api.de/api/interpreter",
                new StringContent(query));

            if (!response.IsSuccessStatusCode)
                return HandleErrorResponse(response.StatusCode);

            var content = await response.Content.ReadAsStringAsync();
            if (content.StartsWith("<"))
                return (null, "Ошибка сервера: получен HTML вместо JSON");

            return ParseResponse(content, lat, lon);
        }
        catch (Exception ex)
        {
            File.AppendAllText("errors.log", $"{DateTime.Now}: {ex}\n");
            return (null, $"Исключение: {ex.Message}");
        }
    }

    static (Settlement, string) ParseResponse(string json, double srcLat, double srcLon)
    {
        try
        {
            var data = JObject.Parse(json);
            var elements = data["elements"] as JArray;
            if (elements == null || elements.Count == 0)
                return (null, "Поселений не найдено");

            Settlement nearest = null;
            double minDist = double.MaxValue;

            foreach (var el in elements)
            {
                var (lat, lon) = GetCoordinates(el);
                if (!lat.HasValue || !lon.HasValue) continue;

                var tags = el["tags"];
                var name = tags?["name"]?.ToString();
                var type = tags?["place"]?.ToString();
                long? population = null;

                if (tags?["population"] != null &&
                    long.TryParse(tags["population"].ToString(), out long pop))
                {
                    population = pop;
                }

                if (string.IsNullOrEmpty(name)) continue;

                var dist = HaversineDistance(srcLat, srcLon, lat.Value, lon.Value);
                if (dist < minDist)
                {
                    minDist = dist;
                    nearest = new Settlement
                    {
                        Name = name,
                        Type = type ?? "unknown",
                        Latitude = lat.Value,
                        Longitude = lon.Value,
                        Distance = dist,
                        Population = population
                    };
                }
            }

            return nearest != null
                ? (nearest, null)
                : (null, "Не удалось распознать поселения");
        }
        catch (Exception ex)
        {
            File.AppendAllText("parse_errors.log", $"{DateTime.Now}: {json}\n{ex}\n");
            return (null, "Ошибка парсинга ответа");
        }
    }

    static (double? lat, double? lon) GetCoordinates(JToken el)
    {
        if (el["lat"] != null && el["lon"] != null)
            return (el["lat"].Value<double>(), el["lon"].Value<double>());

        var center = el["center"];
        if (center != null)
            return (center["lat"].Value<double>(), center["lon"].Value<double>());

        return (null, null);
    }

    static (Settlement, string) HandleErrorResponse(HttpStatusCode statusCode)
    {
        return statusCode switch
        {
            HttpStatusCode.TooManyRequests => (null, "Слишком много запросов - попробуйте позже"),
            HttpStatusCode.GatewayTimeout => (null, "Таймаут сервера"),
            _ => (null, $"HTTP ошибка: {(int)statusCode}")
        };
    }

    static double NormalizeLongitude(double lon)
    {
        lon %= 360;
        return lon switch
        {
            < -180 => lon + 360,
            > 180 => lon - 360,
            _ => lon
        };
    }

    static double NormalizeLatitude(double lat) => Math.Clamp(lat, -90, 90);

    static double HaversineDistance(double lat1, double lon1, double lat2, double lon2)
    {
        const double R = 6371;
        var dLat = ToRadians(lat2 - lat1);
        var dLon = ToRadians(lon2 - lon1);
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(ToRadians(lat1)) * Math.Cos(ToRadians(lat2)) *
                Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        return R * (2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a)));
    }

    static double ToRadians(double degrees) => degrees * Math.PI / 180;

    static void SaveResults(List<(Point Point, Settlement Settlement, string ErrorMessage)> results, string originalPath)
    {
        var outputPath = Path.Combine(
            Path.GetDirectoryName(originalPath),
            $"Results_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}.xlsx");

        using (var wb = new XLWorkbook())
        {
            var ws = wb.Worksheets.Add("Результаты");

            // Заголовки
            ws.Cell("A1").Value = "Исходная долгота";
            ws.Cell("B1").Value = "Исходная широта";
            ws.Cell("C1").Value = "Название";
            ws.Cell("D1").Value = "Тип";
            ws.Cell("E1").Value = "Долгота поселения";
            ws.Cell("F1").Value = "Широта поселения";
            ws.Cell("G1").Value = "Расстояние (км)";
            ws.Cell("H1").Value = "Население";
            ws.Cell("I1").Value = "Статус";

            // Данные
            int row = 2;
            foreach (var r in results)
            {
                ws.Cell(row, 1).Value = r.Point.Longitude;
                ws.Cell(row, 2).Value = r.Point.Latitude;

                if (r.Settlement != null)
                {
                    ws.Cell(row, 3).Value = r.Settlement.Name;
                    ws.Cell(row, 4).Value = r.Settlement.Type;
                    ws.Cell(row, 5).Value = r.Settlement.Longitude;
                    ws.Cell(row, 6).Value = r.Settlement.Latitude;
                    ws.Cell(row, 7).Value = Math.Round(r.Settlement.Distance, 2);
                    ws.Cell(row, 8).Value = r.Settlement.Population.HasValue
     ? r.Settlement.Population.Value
     : "Н/Д";
                    ws.Cell(row, 9).Value = "OK";
                }
                else
                {
                    ws.Cell(row, 9).Value = r.ErrorMessage;
                    ws.Row(row).Style.Fill.BackgroundColor = XLColor.LightPink;
                }
                row++;
            }

            ws.Columns().AdjustToContents();
            wb.SaveAs(outputPath);
        }

        Console.WriteLine($"\nРезультаты сохранены: {outputPath}");
    }
}

class Point
{
    public double Longitude { get; }
    public double Latitude { get; }

    public Point(double lon, double lat)
    {
        Longitude = lon;
        Latitude = lat;
    }
}

class Settlement
{
    public string Name { get; set; }
    public string Type { get; set; }
    public double Longitude { get; set; }
    public double Latitude { get; set; }
    public double Distance { get; set; }
    public long? Population { get; set; }
}