using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace azure_cosmosdb_rbac_access
{
    // <Model>
    public class Item
    {
        public string id { get; set; }
        public string pk { get; set; }
        public int counter { get; set; }
        public string timestamp { get; set; }

        //system properties
        public string _rid { get; set; }
        public string _self { get; set; }
        public string _etag { get; set; }
        public string _attachments { get; set; }
        public int _ts { get; set; }
    }
    // </Model>
}
