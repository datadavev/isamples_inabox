import typing
import requests
import geojson
import fastapi
import logging
import os.path
import urllib.parse
import isb_web.config

BASE_URL = isb_web.config.Settings().solr_url
_RPT_FIELD = "producedBy_samplingSite_location_rpt"

# Identify the bounding boxes for solr and leaflet for diagnostic purposes
SOLR_BOUNDS = -1
LEAFLET_BOUNDS = -2

MIN_LAT = "min_lat"
MAX_LAT = "max_lat"
MIN_LON = "min_lon"
MAX_LON = "max_lon"

# 0.2 seems ok for the grid cells.
_GEOJSON_ERR_PCT = 0.2
# 0.1 for the leaflet heatmap tends to generate more cells for the heatmap “blob” generation
_LEAFLET_ERR_PCT = 0.1

# Maximum rows to return in a streaming request.
MAX_STREAMING_ROWS = 500000

RESERVED_CHAR_LIST = [
    "+",
    "-",
    "&",
    "|",
    "!",
    "(",
    ")",
    "{",
    "}",
    "[",
    "]",
    "^",
    '"',
    "~",
    "*",
    "?",
    ":",
]


def escape_solr_query_term(term):
    """Escape a query term for inclusion in a query.
    """
    term = term.replace("\\", "\\\\")
    for c in RESERVED_CHAR_LIST:
        term = term.replace(c, r"\{}".format(c))
    return term


def clip_float(v, min_v, max_v):
    if v < min_v:
        return min_v
    if v > max_v:
        return max_v
    return v


def get_solr_url(path_component: str):
    return urllib.parse.urljoin(BASE_URL, path_component)


def _get_heatmap(
    q: typing.AnyStr,
    bb: typing.Dict,
    dist_err_pct: float,
    fq: typing.AnyStr = "",
    grid_level=None,
) -> typing.Dict:
    # TODO: dealing with the antimeridian ("dateline") in the Solr request.
    # Should probably do this by computing two requests when the request BB
    # straddles the antimeridian.
    if bb is None or len(bb) < 2:
        bb = {MIN_LAT: -90.0, MAX_LAT: 90.0, MIN_LON: -180.0, MAX_LON: 180.0}
    bb[MIN_LAT] = clip_float(bb[MIN_LAT], -90.0, 90.0)
    bb[MAX_LAT] = clip_float(bb[MAX_LAT], -90.0, 90.0)
    bb[MIN_LON] = clip_float(bb[MIN_LON], -180.0, 180.0)
    bb[MAX_LON] = clip_float(bb[MAX_LON], -180.0, 180.0)
    # logging.warning(bb)
    headers = {"Accept": "application/json"}
    params = {
        "q": q,
        "rows": 0,
        "wt": "json",
        "facet": "true",
        "facet.heatmap": _RPT_FIELD,
        "facet.heatmap.distErrPct": dist_err_pct,
        # "facet.heatmap.gridLevel": grid_level,
        "facet.heatmap.geom": f"[{bb[MIN_LON]} {bb[MIN_LAT]} TO {bb[MAX_LON]} {bb[MAX_LAT]}]",
    }
    if not fq is None:
        params["fq"] = fq
    # if grid level is None, then Solr calculates an "appropriate" grid scale
    # based on the bounding box and distErrPct. Seems a bit off...
    if grid_level is not None:
        params["facet.heatmap.gridLevel"] = grid_level
    # Get the solr heatmap for the provided bounds
    url = get_solr_url("select")
    response = requests.get(url, headers=headers, params=params)

    # logging.debug("Got: %s", response.url)
    res = response.json()
    total_matching = res.get("response", {}).get("numFound", 0)
    hm = res.get("facet_counts", {}).get("facet_heatmaps", {}).get(_RPT_FIELD, {})
    hm["numDocs"] = total_matching
    return hm


##
# Create a GeoJSON rendering of the Solr Heatmap response.
# Generates a GeoJSON polygon (rectangle) feature for each Solr heatmap cell
# that has a count value over 0.
# Returns the generated features as a GeoJSON FeatureCollection,
# https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
def solr_geojson_heatmap(
    q, bb, fq=None, grid_level=None, show_bounds=False, show_solr_bounds=False
):
    hm = _get_heatmap(q, bb, _GEOJSON_ERR_PCT, fq=fq, grid_level=grid_level)
    # print(hm)
    gl = hm.get("gridLevel", -1)
    # logging.warning(hm)
    d_lat = hm["maxY"] - hm["minY"]
    dd_lat = d_lat / (hm["rows"])
    d_lon = hm["maxX"] - hm["minX"]
    dd_lon = d_lon / (hm["columns"])
    lat_0 = hm["maxY"]  # - dd_lat
    lon_0 = hm["minX"]  # + dd_lon
    _max_value = 0

    # Container for the generated geojson features
    grid = []
    if show_bounds:
        bbox = geojson.Feature(
            geometry=geojson.Polygon(
                [
                    [
                        (
                            bb[MIN_LAT],
                            bb[MIN_LON],
                        ),
                        (
                            bb[MAX_LAT],
                            bb[MIN_LON],
                        ),
                        (
                            bb[MAX_LAT],
                            bb[MAX_LON],
                        ),
                        (
                            bb[MIN_LAT],
                            bb[MAX_LON],
                        ),
                        (
                            bb[MIN_LAT],
                            bb[MIN_LON],
                        ),
                    ]
                ]
            ),
            properties={"count": LEAFLET_BOUNDS},
        )
        grid.append(bbox)
    if show_solr_bounds:
        bbox = geojson.Feature(
            geometry=geojson.Polygon(
                [
                    [
                        (
                            hm["minX"],
                            hm["minY"],
                        ),
                        (
                            hm["maxX"],
                            hm["minY"],
                        ),
                        (
                            hm["maxX"],
                            hm["maxY"],
                        ),
                        (
                            hm["minX"],
                            hm["maxY"],
                        ),
                        (
                            hm["minX"],
                            hm["minY"],
                        ),
                    ]
                ]
            ),
            properties={"count": SOLR_BOUNDS},
        )
        grid.append(bbox)

    # Process the Solr heatmap response. Draw a box for each cell
    # that has a count > 0 and set the "count" property of the
    # feature to that value.
    _total = 0
    _count_matrix = hm.get("counts_ints2D", None)
    if _count_matrix is not None:
        for i_row in range(0, hm["rows"]):
            for i_col in range(0, hm["columns"]):
                if _count_matrix[i_row] is not None:
                    v = _count_matrix[i_row][i_col]
                    if v > 0:
                        _total = _total + v
                        if v > _max_value:
                            _max_value = v
                        p0lat = lat_0 - dd_lat * i_row
                        p0lon = lon_0 + dd_lon * i_col
                        pts = geojson.Polygon(
                            [
                                [
                                    (
                                        p0lon,
                                        p0lat,
                                    ),
                                    (
                                        p0lon + dd_lon,
                                        p0lat,
                                    ),
                                    (
                                        p0lon + dd_lon,
                                        p0lat - dd_lat,
                                    ),
                                    (
                                        p0lon,
                                        p0lat - dd_lat,
                                    ),
                                    (
                                        p0lon,
                                        p0lat,
                                    ),
                                ]
                            ]
                        )
                        feature = geojson.Feature(geometry=pts, properties={"count": v})
                        grid.append(feature)
    geodata = geojson.FeatureCollection(grid)
    geodata["max_count"] = _max_value
    geodata["grid_level"] = gl
    geodata["total"] = _total
    geodata["num_docs"] = hm.get("numDocs", 0)
    return geodata


# Generate a list of [latitude, longitude, value] from
# a solr heatmap. Latitude and longitude represent the
# centers of the solr heatmap grid cells. The value is the count
# for the grid cell.
# Suitable for consumption by leaflet: https://leafletjs.com
def solr_leaflet_heatmap(q, bb, fq=None, grid_level=None):
    hm = _get_heatmap(q, bb, _LEAFLET_ERR_PCT, fq=fq, grid_level=grid_level)
    # logging.warning(hm)
    d_lat = hm["maxY"] - hm["minY"]
    dd_lat = d_lat / (hm["rows"])
    d_lon = hm["maxX"] - hm["minX"]
    dd_lon = d_lon / (hm["columns"])
    lat_0 = hm["maxY"] - dd_lat / 2.0
    lon_0 = hm["minX"] + dd_lon / 2.0
    data = []
    max_value = 0
    _total = 0
    for i_row in range(0, hm["rows"]):
        for i_col in range(0, hm["columns"]):
            if hm["counts_ints2D"][i_row] is not None:
                v = hm["counts_ints2D"][i_row][i_col]
                if v > 0:
                    _total = _total + v
                    lt = lat_0 - dd_lat * i_row
                    lg = lon_0 + dd_lon * i_col
                    data.append([lt, lg, v])
                    if v > max_value:
                        max_value = v
    # return list of [lat, lon, count] and maximum count value
    return {
        "data": data,
        "max_value": max_value,
        "total": _total,
        "num_docs": hm.get("numDocs", 0),
    }


def solr_query(params, query=None):
    """
    Issue a request against the solr select endpoint.

    Params is a list of [k, v] to support duplicate keys, which solr
    uses a lot of.

    Args:
        params: list of list, see https://solr.apache.org/guide/8_9/common-query-parameters.html

    Returns:
        Iterator for the solr response.
    """
    url = get_solr_url("select")
    headers = {"Accept": "application/json"}
    content_type = "application/json"
    wt_map = {
        "csv": "text/plain",
        "xml": "application/xml",
        "geojson": "application/geo+json",
        "smile": "application/x-jackson-smile",
        "json": "application/json",
    }
    for k, v in params:
        if k == "wt":
            content_type = wt_map.get(v.lower(), "json")
    if query is None:
        response = requests.get(url, headers=headers, params=params, stream=True)
    else:
        response = requests.post(url, headers=headers, params=params, json=query, stream=True)
    return fastapi.responses.StreamingResponse(
        response.iter_content(chunk_size=2048), media_type=content_type
    )

def solr_get_record(identifier):
    """
    Retrieve the solr document for the specified identifier.

    The select endpoint is used instead of get because get does
    not return fields populated by copyField operations.

    Args:
        identifier: string, the record identifier

    Returns: status_code, object
    """
    params = {
        "wt": "json",
        "q": f"id:{escape_solr_query_term(identifier)}",
        "fl": "*",
        "rows": 1,
        "start": 0,
    }
    url = get_solr_url("select")
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        return response.status_code, None
    docs = response.json()
    if docs['response']['numFound'] == 0:
        return 404, None
    return 200, docs['response']['docs'][0]



def solr_searchStream(params, collection="isb_core_records"):
    """
    Requests a streaming search response from solr.

    This is experimental, with the ultimate goal of supporting rendering of
    a large (100k or so) points to the Cesium display for arbitrary queries.

    Example with curl:
    curl --data-urlencode 'expr=search(isb_core_records,q="source:SESAR",fq="searchText:sample",fq="hasMaterialCategory:Mineral",fl="id,producedBy_samplingSite_location_latitude,producedBy_samplingSite_location_longitude",sort="id asc",qt="/export")' "http://localhost:8985/solr/isb_core_records/stream"

    Args:
        params: parameters for the search expression
        collection: name of collection to search

    Returns:

    """
    #TODO: Test coverage

    url = get_solr_url("stream")
    headers = {"Accept": "application/json"}
    qparams = {}
    _params = []
    _has_rows = False
    _has_q = False
    _has_sort = False
    _has_fl = False
    for k,v in params.items():
        if k == "rows":
            _has_rows = True
            if int(v) > MAX_STREAMING_ROWS:
                v = MAX_STREAMING_ROWS
        if k == "sort":
            _has_sort = True
        if k == "fl":
           _fields = v.split(",")
           _fields.append("x:producedBy_samplingSite_location_longitude")
           _fields.append("y:producedBy_samplingSite_location_latitude")
           v = ",".join(_fields)
        _params.append(f'{k}="{v}"')
    if not _has_rows:
        _params.append(f'rows={MAX_STREAMING_ROWS}')
    if not _has_sort:
        _params.append('sort="id asc"')
    if not _has_q:
        _params.append('q="*:*"')
    if not _has_fl:
        _params.append('fl="id,x:producedBy_samplingSite_location_longitude,y:producedBy_samplingSite_location_latitude"')
    _params.append(('fq="producedBy_samplingSite_location_longitude:*'
                    ' AND producedBy_samplingSite_location_latitude:*"'))
    content_type = "application/json"
    request = {
        "expr":
            (f'select(rollup(search({collection},{",".join(_params)},qt="/select")'
             ',over="x,y",count(*)),x,y,count(*) as n)')
    }
    logging.info("Expression = %s", request["expr"])
    response = requests.post(url, headers=headers, params=qparams, data=request, stream=True)
    logging.info("Returning response")
    return fastapi.responses.StreamingResponse(
        response.iter_content(chunk_size=4096), media_type=content_type
    )


def solr_luke():
    """
    Information about the solr isb_core_records schema
    See: https://solr.apache.org/guide/8_9/luke-request-handler.html

    Returns:
        JSON document iterator
    """
    url = get_solr_url("admin/luke")
    params = {"show": "schema", "wt": "json"}
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers, params=params, stream=True)
    return fastapi.responses.StreamingResponse(
        response.iter_content(chunk_size=2048), media_type="application/json"
    )


def _fetch_solr_records(
    rsession=requests.session(),
    authority_id: typing.Optional[str] = None,
    start_index: int = 0,
    batch_size: int = 50000,
    field: typing.Optional[str] = None,
    sort: typing.Optional[str] = None
):
    headers = {"Content-Type": "application/json"}
    if authority_id is None:
        query = "*:*"
    else:
        query = f"source:{authority_id}"
    params = {
        "q": query,
        "rows": batch_size,
        "start": start_index,
    }
    if field is not None:
        params["fl"] = field
    if sort is not None:
        params["sort"] = sort
    _url = get_solr_url("select")
    res = rsession.get(_url, headers=headers, params=params)
    json = res.json()
    docs = json["response"]["docs"]
    return docs


def solr_records_for_sitemap(
    rsession=requests.session(),
    authority_id: typing.Optional[str] = None,
    start_index: int = 0,
    batch_size: int = 50000,
) -> typing.List[typing.Dict]:
    """

    Args:
        rsession: The requests.session object to use for sending the solr request
        authority_id: The authority_id to use when querying SOLR, defaults to all
        start_index: The offset for the records to return
        batch_size: Number of documents for this particular sitemap document

    Returns:
        A list of dictionaries of solr documents with id and sourceUpdatedTime fields
    """
    return _fetch_solr_records(rsession, authority_id, start_index, batch_size, "id,sourceUpdatedTime", "sourceUpdatedTime asc")



class ISBCoreSolrRecordIterator:
    """
    Iterator class for looping over all the Solr records in the ISB core Solr schema
    """

    def __init__(
        self,
        rsession = requests.session(),
        authority: str = None,
        batch_size: int = 50000,
        offset: int = 0,
        sort: str = None
    ):
        """

        Args:
            rsession: The requests.session object to use for sending the solr request
            authority_id: The authority_id to use when querying SOLR, defaults to all
            batch_size: Number of documents to fetch at a time
            offset: The offset into the records to begin iterating
            sort: The solr sort parameter to use
        """
        self.rsession = rsession
        self.authority = authority
        self.batch_size = batch_size
        self.offset = offset
        self.sort = sort
        self._current_batch = []
        self._current_batch_index = -1


    def __iter__(self):
        return self


    def __next__(self) -> typing.Dict:
        if len(self._current_batch) == 0 or self._current_batch_index == len(self._current_batch):
            self._current_batch = _fetch_solr_records(self.rsession, self.authority, self.offset, self.batch_size, None, self.sort)
            if len(self._current_batch) == 0:
                # reached the end of the records
                raise StopIteration
            logging.info(f"Just fetched {len(self._current_batch)} ISB Core solr records at offset {self.offset}")
            self.offset += self.batch_size
            self._current_batch_index = 0
        # return the next one in the list and increment our index
        next_record = self._current_batch[self._current_batch_index]
        self._current_batch_index = self._current_batch_index + 1
        return next_record