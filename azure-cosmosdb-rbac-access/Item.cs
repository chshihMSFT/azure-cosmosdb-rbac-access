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

    public class GraphVertex
    {
        public string label { get; set; }
        public string id { get; set; }
        public string pk { get; set; }
        public List<GraphVertexProp> counter { get; set; }
        public List<GraphVertexProp> timestamp { get; set; }
        public List<GraphVertexProp> property01 { get; set; }

        //system properties
        public string _rid { get; set; }
        public string _self { get; set; }
        public string _etag { get; set; }
        public string _attachments { get; set; }
        public int _ts { get; set; }
    }

    public class GraphVertexProp
    {
        public string id { get; set; }
        public string _value { get; set; }
    }
    public class GraphEdge
    {
        public string label { get; set; }
        public string id { get; set; }
        public string pk { get; set; }
        public bool _isEdge { get; set; }

        //----------------------------------------
        public string _sink { get; set; }
        public string _sinkLabel { get; set; }
        public string _sinkPartition { get; set; }
        public string _vertexId { get; set; }
        public string _vertexLabel { get; set; }
        //----------------------------------------

        //system properties
        public string _rid { get; set; }
        public string _self { get; set; }
        public string _etag { get; set; }
        public string _attachments { get; set; }
        public int _ts { get; set; }
    }
    // </Model>
}
