// Name of the field for data sources
const SOURCE="source";

// Fields for faceting
const DEFAULT_FACETS = [
    "hasMaterialCategory",
    "hasSpecimenCategory",
    "hasContextCategory",
]

MISSING_VALUE = "-9999";

const FORMAT="0,0";

function nFormat(v) {
    if (v === undefined) {
        return "";
    }
    if (v === MISSING_VALUE){
        return v;
    }
    return numeral(v).format(FORMAT);
}

// Get a value from the solr pivot table list of lists
function getPivotValue(pdata, f0, f1) {
    for (var p=0; p<pdata.length; p++) {
        if (pdata[p].value === f0) {
            let _pivot = pdata[p].pivot;
            if (_pivot === undefined) {
                return 0;
            }
            for (var i=0; i<_pivot.length; i++) {
                if (_pivot[i].value === f1) {
                    return _pivot[i].count;
                }
            }
            return 0;
        }
    }
    return 0;
}

// Get a pivot total value from a solr pivot table
function getPivotTotal(pdata, f0) {
    for (var p=0; p<pdata.length; p++) {
        if (pdata[p].value === f0) {
            return pdata[p].count;
        }
    }
    return 0;
}

async function getSolrRecordSummary(query = "*:*", facets = DEFAULT_FACETS) {
    const TOTAL = "Total";
    var _url = new URL("/thing/select", document.location);
    let params = _url.searchParams;
    params.append("q", query);
    params.append("facet", "on");
    params.append("facet.method", "enum");
    params.append("wt", "json");
    params.append("rows", 0);
    params.append("facet.field", SOURCE);
    for (var i = 0; i < facets.length; i++) {
        params.append("facet.field", facets[i]);
        params.append("facet.pivot", SOURCE+"," + facets[i]);
    }
    let response = await fetch(_url);
    let data = await response.json();
    // container for later display in UI
    let facet_info = {
        // list of fields that were faceted
        fields: facets,
        // total number of records that matched query
        total_records: 0,
        // List of source names
        sources: [],
        // keyed by facet field name
        facets: {},
        // total records keyed by source
        totals: {}
    }
    facet_info.total_records = data.response.numFound;
    for (var i = 0; i < data.facet_counts.facet_fields[SOURCE].length; i += 2) {
        facet_info.sources.push(data.facet_counts.facet_fields[SOURCE][i]);
        facet_info.totals[data.facet_counts.facet_fields[SOURCE][i]] = data.facet_counts.facet_fields[SOURCE][i+1]
    }
    for (const f in data.facet_counts.facet_fields) {
        /*
        entry will looks like:
        {
            _keys: [SESAR, ..., "total"],
            facet_value: {SESAR:count, ..., total:count},
            ...
            facet_value: ...,
            Total: ...
        }
         */
        if (f === SOURCE) {
            continue;
        }
        let entry = {_keys: []};
        let columns = facet_info.sources;
        let _pdata = data.facet_counts.facet_pivot[SOURCE+","+f];
        for (var i = 0; i < data.facet_counts.facet_fields[f].length; i += 2) {
            let k = data.facet_counts.facet_fields[f][i];
            entry._keys.push(k);
            entry[k] = {};
            entry[k][TOTAL] = data.facet_counts.facet_fields[f][i + 1];
            for (const col in columns) {
                entry[k][columns[col]] = getPivotValue(_pdata, columns[col], k);
            }
        }
        entry._keys.push(TOTAL);
        entry[TOTAL] = {
            TOTAL: MISSING_VALUE
        };
        for (const col in columns) {
            entry[TOTAL][columns[col]] = getPivotTotal(_pdata, columns[col]);
        }
        facet_info.facets[f] = entry;
    }
    return facet_info
}

/*
Provide the overview data to the Alpine.js driven UI.
 */
function dataSummary() {
    return {
        query: "*:*",
        total_records: 0,
        sources: [],
        fields: [],
        facets: {
            source: [{}, {}, {}, {}]
        },
        // Update values, such as when the query changes
        update(){
            getSolrRecordSummary(this.query, DEFAULT_FACETS).then((res) => {
                this.fields = res.fields;
                this.totals = res.totals;
                this.total_records = res.total_records;
                this.facets = res.facets;
                this.sources = res.sources;
            });
        },
        // Called on initialization of the component
        init() {
            const _qry = new URLSearchParams(window.location.search);
            const _params = Object.fromEntries(_qry.entries());
            this.query = _params.q || this.query;
            this.update();
        }
    }
}

/*
Gather some basic info about the repo.
 */
function siteInfo() {
    return {
        _info: "",
        init() {
            const url = "https://api.github.com/repos/isamplesorg/isamples_inabox/commits/develop";
            fetch(url)
                .then(response => response.json())
                .then(data => {
                    let _sha = data.sha.substr(0,7);
                    var dmod = data.commit.author.date;
                    this._info = "Revision " + _sha + " at " + dmod;
                })
        }
    }
}

function onLoad() {
    //place holder to call when body is loaded
}