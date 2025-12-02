using ETL.OpinionesWorker;
using ETL.OpinionesWorker.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<Worker>();
builder.Services.AddSingleton<DataLoader>();
builder.Services.AddHttpClient();
builder.Services.AddTransient<DimensionLoader>();

var host = builder.Build();
host.Run();