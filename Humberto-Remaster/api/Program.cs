using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();
var path = builder.Configuration["GoldSnapshot"] ?? "../data/gold/snapshot.json";
using var snapshot = JsonDocument.Parse(File.ReadAllText(path));
var gold = snapshot.RootElement;
var report = gold.GetProperty("report");
var version = report.GetProperty("version").GetString();
var stations = gold.GetProperty("stations").EnumerateArray()
    .ToDictionary(row => row.GetProperty("station_id").GetInt32());
var lines = gold.GetProperty("lines").EnumerateArray()
    .ToDictionary(row => row.GetProperty("line_id").GetInt32());
var pilotPath = Path.Combine(app.Environment.ContentRootPath, "pilot-routes.json");
var pilot = JsonSerializer.Deserialize<Pilot>(File.ReadAllText(pilotPath), new JsonSerializerOptions(JsonSerializerDefaults.Web))
    ?? throw new InvalidOperationException("Arquivo de rotas do piloto inválido.");
if (pilot.Routes.Any(route => route.Rides.Any(ride => ride.StationIds.Any(id =>
    !stations.TryGetValue(id, out var station) || station.GetProperty("line_id").GetInt32() != ride.LineId))))
{
    throw new InvalidOperationException("Uma estação do piloto não corresponde à versão Gold.");
}

app.UseDefaultFiles();
app.UseStaticFiles();
app.MapGet("/api/health", () => new { status = "ok", version });
app.MapGet("/api/stations", () => new { version, items = stations.Values });
app.MapGet("/api/stations/{id:int}", (int id) =>
    stations.TryGetValue(id, out var station) ? Results.Ok(new { version, item = station }) : Results.NotFound());
app.MapGet("/api/lines", () => new { version, items = lines.Values });
app.MapGet("/api/lines/{id:int}", (int id) =>
    lines.TryGetValue(id, out var line) ? Results.Ok(new { version, item = line }) : Results.NotFound());
app.MapGet("/api/pilot", () => new { pilot.Origin, pilot.Destination, source = pilot.Source });
app.MapPost("/api/routes/compare", (RouteRequest request) =>
{
    var forward = request.Origin == pilot.Origin && request.Destination == pilot.Destination;
    var reverse = request.Origin == pilot.Destination && request.Destination == pilot.Origin;
    if (!forward && !reverse)
    {
        return Results.BadRequest(new { message = $"O piloto cobre {pilot.Origin} ↔ {pilot.Destination}." });
    }
    if (request.Preference is not ("fewer_transfers" or "fewer_stops"))
    {
        return Results.BadRequest(new { message = "Escolha menos baldeações ou menos paradas." });
    }

    var options = pilot.Routes.Select(option =>
    {
        var rides = reverse ? option.Rides.Reverse() : option.Rides;
        return new RouteOption(
            option.Id,
            option.Rides.Length - 1,
            option.Rides.Sum(ride => ride.StationIds.Length - 1),
            rides.Select(ride => new RouteLeg(
                ride.LineId,
                (reverse ? ride.StationIds.Reverse() : ride.StationIds)
                    .Select(id => stations[id]).ToArray())).ToArray(),
            "sem verificação de acessibilidade no trajeto");
    });
    var ordered = request.Preference == "fewer_stops"
        ? options.OrderBy(route => route.Stops).ThenBy(route => route.Transfers)
        : options.OrderBy(route => route.Transfers).ThenBy(route => route.Stops);
    return Results.Ok(new { version, options = ordered.ToArray(), radiusM = report.GetProperty("radius_m") });
});
app.Run();

record RouteRequest(string Origin, string Destination, string Preference);
record Pilot(string Origin, string Destination, string Source, PilotRoute[] Routes);
record PilotRoute(string Id, PilotRide[] Rides);
record PilotRide(int LineId, int[] StationIds);
record RouteOption(string Id, int Transfers, int Stops, RouteLeg[] Legs, string EvidenceStatus);
record RouteLeg(int LineId, JsonElement[] Stations);
