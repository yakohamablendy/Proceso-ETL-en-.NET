using ETL.OpinionesWorker;
using ETL.OpinionesWorker.Services;

var builder = Host.CreateApplicationBuilder(args);


builder.Services.AddHostedService<Worker>();
builder.Services.AddHttpClient();
builder.Services.AddSingleton<DataLoader>();
builder.Services.AddTransient<DimensionLoader>();
builder.Services.AddTransient<FactLoader>();

var host = builder.Build();
host.Run();