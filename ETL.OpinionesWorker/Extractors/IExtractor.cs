using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ETL.OpinionesWorker.Models;

namespace ETL.OpinionesWorker.Extractors
{
    public interface IExtractor
    {
        Task<List<OpinionData>> ExtractAsync();
        string GetSourceName();
    }
}
